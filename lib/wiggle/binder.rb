# frozen_string_literal: true

require "set"

module Wiggle
  # Both context snapshots of the step being undone, captured by the engine at the step's completion
  # -- NOT read from the instance's latest context, which a later step may have replaced. +result+ is
  # the primary snapshot for most undos (the step's own products live there); +input+ serves
  # restore-previous-value undos and undo-only data (an idempotency key derived from the input).
  Compensation = Struct.new(:input, :result, keyword_init: true)

  # Optional base for a class whose methods implement a workflow's steps -- one public method per
  # step, each taking the context. Register an instance with Worker#register_handlers.
  #
  # Method names match step names REGARDLESS OF CASE STYLE: +in_stock+, +inStock+ and a graph step
  # named +in-stock+ all fold the same, and a trailing +?+ is ignored, so +in_stock?+ matches too.
  # The graph decides gate vs task; mark side-effect methods (whose return is ignored and the context
  # left unchanged) with +effect+. A method named +compensate_<step>+ is the undo of a compensable
  # step and receives a Wiggle::Compensation. Private and protected methods are helpers, not steps.
  #
  #   class OrderHandlers < Wiggle::Handlers
  #     workflow "order-fulfilment"
  #
  #     def validate(o) = o.merge("status" => "VALIDATED")    # a task: returns the new context
  #     def in_stock?(o) = o["qty"].positive?                  # matches the gate "in-stock"
  #     effect def notify(o) = puts("shipped #{o['orderId']}")  # a side effect: context unchanged
  #
  #     private
  #
  #     def receipt(o) = ...                                   # a helper, not a step handler
  #   end
  class Handlers
    class << self
      # Set (or read) the workflow these handlers implement.
      def workflow(name = nil)
        return @workflow = name unless name.nil?

        @workflow || (superclass.respond_to?(:workflow) ? superclass.workflow : nil)
      end

      # Mark methods as side effects. Returns its argument, so +effect def notify(o) ... end+ works.
      def effect(*names)
        own_effects.merge(names.flatten.map(&:to_s))
        names.size == 1 ? names.first : names
      end

      # Every method marked with +effect+ on this class or an ancestor.
      def effects
        inherited = superclass.respond_to?(:effects) ? superclass.effects : Set.new
        inherited | own_effects
      end

      private

      def own_effects = (@own_effects ||= Set.new)
    end

    def workflow = self.class.workflow

    def effects = self.class.effects
  end

  # The worker's reflective seam: turns a handlers object into executable step wrappers.
  #
  # Two pure operations, deliberately free of I/O and of the worker's runtime state, so every
  # signature rule here is unit-testable against a compiled graph (mirrors the Java client's
  # HandlerBinder):
  #
  # - Binder.scan -- inventory an object's step methods (by canonical name), rejecting ambiguous
  #   names and non-single-context signatures.
  # - Binder.bind -- resolve an inventory against a graph, node by node, validating each matched
  #   method against the node's kind and building the invocation wrapper. Returns the bindings plus
  #   the steps this object doesn't serve; the Worker decides what to do with both.
  # - Binder.unstage -- turn a combine activation's context into the shape a combine handler takes,
  #   and compute the base Wiggle::Current.base exposes inside it.
  module Binder
    Candidate = Struct.new(:name, :method, :effect, keyword_init: true)
    Binding = Struct.new(:activity, :step, :queue, :handler, keyword_init: true)
    Result = Struct.new(:bindings, :unserved, keyword_init: true)

    # Methods every object answers that are never step handlers.
    RESERVED = %w[workflow effects].freeze

    module_function

    # Fold a name to a case/style-independent key: lowercase alphanumeric tokens, concatenated. So
    # +in-stock+, +in_stock+, +inStock+, +InStock+, +in_stock?+ and +instock+ all yield +instock+.
    def canonical(name)
      name.to_s
          .gsub(/[^0-9A-Za-z]+/, " ")
          .gsub(/(?<=[a-z0-9])(?=[A-Z])/, " ")
          .gsub(/(?<=[A-Z])(?=[A-Z][a-z])/, " ")
          .split.map(&:downcase).join
    end

    # Introspect +handlers+ for its step methods, keyed by canonical name. Raises if two public
    # methods fold to the same canonical name (ambiguous across case styles), or if a public method
    # does not take a single context argument. +effects+ names extra side-effect methods (for an
    # object that is not a Wiggle::Handlers).
    def scan(handlers, effects: [])
      effect_names = Set.new(effects.map(&:to_s))
      effect_names.merge(handlers.effects.map(&:to_s)) if handlers.is_a?(Handlers)
      found = {}
      step_method_names(handlers).sort.each do |attr|
        m = handlers.method(attr)
        unless single_context?(m)
          raise ArgumentError, "handler method '#{attr}' must accept a single context argument " \
                               "(make helper methods private to skip them)"
        end
        canon = canonical(attr)
        next if canon.empty?

        if found.key?(canon)
          raise ArgumentError, "handler methods '#{found[canon].name}' and '#{attr}' both map to the same step " \
                               "name '#{canon}'; names differing only in case/style are ambiguous -- rename one"
        end
        found[canon] = Candidate.new(name: attr.to_s, method: m, effect: effect_names.include?(attr.to_s))
      end
      unknown = effect_names - found.values.map(&:name)
      raise ArgumentError, "effect #{unknown.to_a.sort.inspect} names no public handler method" if unknown.any?
      raise ArgumentError, "no handler methods found on the object (public methods taking one context argument)" if found.empty?

      found
    end

    # Resolve an inventory against a compiled graph: each candidate is matched to a worker-dispatched
    # step by canonical name and wrapped per the node's kind. A method matching no step, or a kind
    # clash, fails fast. Pure: no I/O, no worker state.
    def bind(workflow, candidates, graph)
      nodes = {}
      graph.fetch("nodes", []).each do |n|
        nodes[n["name"]] = n if %w[TASK PREDICATE].include?(n["kind"]) && n.key?("name")
      end
      by_canon = {}
      nodes.each { |name, node| by_canon[canonical(name)] ||= [name, node] } # graph step names are already unique

      # Partition out compensators first: a method named compensate_<step> (any case style) whose
      # canonical name matches no step directly is the undo of <step>. A step literally named
      # "compensate-x" keeps priority -- its handler matches by_canon directly and stays forward.
      forward = {}
      compensators = {} # canonical TARGET step -> candidate
      candidates.each do |canon, cand|
        target = canon.start_with?("compensate") ? canon.delete_prefix("compensate") : ""
        if !by_canon.key?(canon) && !target.empty? && by_canon.key?(target)
          compensators[target] = cand
        else
          forward[canon] = cand
        end
      end

      bindings = []
      served = Set.new
      forward.each do |canon, cand|
        match = by_canon[canon]
        if match.nil?
          raise ArgumentError, "handler '#{cand.name}' matches no step in workflow '#{workflow}' " \
                               "(available steps: #{nodes.keys.sort.inspect})"
        end
        step_name, node = match
        activity = "#{workflow}##{step_name}"
        bindings << Binding.new(activity: activity, step: step_name, queue: node.fetch("queue", workflow),
                                handler: wrapper_for(cand, node, activity))
        served << step_name
      end

      # Compensators bind under "<activity>#compensate"; the pairing is checked BOTH ways -- a
      # compensate_<x> targeting a non-compensable step is a lie, and a compensable step served
      # forward without its undo would strand the engine's reverse pass.
      compensators.each do |target, cand|
        step_name, node = by_canon[target]
        unless compensable_task?(node)
          raise ArgumentError, "compensator '#{cand.name}' targets step '#{step_name}' of workflow '#{workflow}', " \
                               "which is not declared compensate: true in the topology"
        end
        bindings << Binding.new(activity: "#{workflow}##{step_name}#compensate", step: step_name,
                                queue: node.fetch("queue", workflow), handler: compensation_wrapper(cand.method))
      end
      forward.each_key do |canon|
        step_name, node = by_canon[canon]
        next if node.nil? || !compensable_task?(node) || compensators.key?(canon)

        raise ArgumentError, "step '#{step_name}' of workflow '#{workflow}' is declared compensate: true but the " \
                             "handlers object has no 'compensate_#{canon}' method taking a Wiggle::Compensation"
      end

      Result.new(bindings: bindings, unserved: (nodes.keys - served.to_a).sort)
    end

    # Adapt a compensator to the runtime handler shape: the engine stages the two snapshots as the
    # activation context {"input" => ..., "result" => ...}; split them out. The return is nil -- an
    # undo never changes the (already doomed) instance context.
    def compensation_wrapper(fn)
      lambda do |ctx|
        ctx = {} unless ctx.is_a?(Hash)
        fn.call(Compensation.new(input: ctx["input"], result: ctx["result"]))
        nil
      end
    end

    # Whether a registered graph node is a fork or forEach combine: a TASK with armNames or a
    # collectKey, or -- in a graph written before those fields -- an itemsKey.
    def combine?(node)
      node["kind"] == "TASK" && [node["armNames"], node["collectKey"], node["itemsKey"]].any? { |v| present?(v) }
    end

    # Rewrite a combine activation's context into the shape a combine handler takes -- each fork arm's
    # result under the arm's name, or a forEach's results under the forEach's name -- and return it
    # with the base (the context without them): +[ctx, base]+, base nil when this is no combine.
    # Raises ArgumentError when such a name is also a key of the workflow's own context, which the
    # handler could not tell apart.
    def unstage(ctx, arm_names, collect_key, step_name)
      return [ctx, nil] if !ctx.is_a?(Hash) || (arm_names.empty? && !present?(collect_key))

      base = ctx.dup
      placed = {}
      place = lambda do |scratch, name|
        next unless base.key?(scratch)

        value = base.delete(scratch)
        if base.key?(name)
          raise ArgumentError, "combine '#{step_name}': '#{name}' is both a result to combine and a key of the " \
                               "context; rename the branch or the key"
        end
        placed[name] = value
      end
      arm_names.each { |arm| place.call(ARM_SCRATCH_PREFIX + arm, arm) }
      if present?(collect_key)
        base.delete(STEPS_SCRATCH_PREFIX + collect_key)
        place.call(collect_key, collect_key.delete_prefix(FOR_EACH_SCRATCH_PREFIX))
      end
      [base.merge(placed), base]
    end

    # Build the runtime wrapper for a matched method, validating it against the graph node's kind
    # (the graph decides gate vs task; the +effect+ mark decides task vs side effect).
    def wrapper_for(cand, node, activity)
      m = cand.method
      predicate_named = cand.name.end_with?("?")
      if node["kind"] == "PREDICATE"
        if cand.effect
          raise ArgumentError, "activity '#{activity}' is a gate (PREDICATE) but handler '#{cand.name}' is marked " \
                               "as an effect; a gate must return a boolean"
        end
        return ->(ctx) { m.call(ctx) ? true : false }
      end
      if predicate_named
        raise ArgumentError, "activity '#{activity}' is a TASK but handler '#{cand.name}' is named like a " \
                             "predicate; that looks like a gate -- did you mean a PREDICATE step?"
      end
      if combine?(node)
        # A fork/forEach combine: the return is the COMPLETE post-join context, sent verbatim (no
        # diff) -- the engine replaces the context with it; there is no implicit fold.
        if cand.effect
          raise ArgumentError, "activity '#{activity}' is a fork combine; handler '#{cand.name}' must return " \
                               "the complete post-join context, so it cannot be an effect"
        end
        return ->(ctx) { m.call(ctx) }
      end
      if cand.effect
        return lambda do |ctx|
          m.call(ctx)
          nil
        end
      end
      # The return is the step's COMPLETE next context: sent whole, it replaces the previous value
      # server-side (nil leaves it untouched).
      ->(ctx) { m.call(ctx) }
    end

    def compensable_task?(node) = node["kind"] == "TASK" && !combine?(node) && node["compensable"] ? true : false

    def present?(v) = !(v.nil? || (v.respond_to?(:empty?) && v.empty?))

    # The public methods an object adds over plain Object (and over Wiggle::Handlers).
    def step_method_names(handlers)
      base = handlers.is_a?(Handlers) ? Handlers.public_instance_methods : Object.public_instance_methods
      (handlers.public_methods - base).map(&:to_s).reject { |n| RESERVED.include?(n) || n.end_with?("=") }
    end

    # True if the method takes exactly one required positional arg (the context) -- or none plus
    # *args. Extra optional/keyword params are fine.
    def single_context?(m)
      required = 0
      rest = false
      m.parameters.each do |type, _|
        case type
        when :req then required += 1
        when :rest then rest = true
        when :keyreq then return false
        end
      end
      required == 1 || (required.zero? && rest)
    end
  end
end
