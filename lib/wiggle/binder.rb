# frozen_string_literal: true

require "set"

module Wiggle
  # The worker's reflective seam: turns a handler object into executable step wrappers. Two pure
  # operations -- no I/O, no worker runtime state -- mirroring the Java client's HandlerBinder:
  #
  # - HandlerBinder.scan -- inventory an object's step methods by canonical name, rejecting
  #   ambiguous names.
  # - HandlerBinder.bind -- resolve an inventory against a graph, node by node, validating each
  #   matched method against the node's kind and building the invocation wrapper. Returns the
  #   bindings plus the steps this object does not serve; the Worker decides what to do with both.
  #
  # A method's shape defines its step. A method taking one argument (the context) serves a task --
  # its return REPLACES the context -- or, on a PREDICATE node, a gate (its truthiness picks the
  # branch). A method marked +void+ (see ForFlow) is an effect: its return is ignored. A method taking
  # no arguments is a FACTORY returning an Activity, GateActivity, EffectActivity or
  # CompensableActivity -- the last being the only way to serve a compensable step. A combine method
  # takes one argument per fork arm, in the order the arms were given to Wiggle.all_of (or the
  # forEach's collected results), plus optionally a trailing argument for the frozen base context;
  # its return is the complete post-join context.
  #
  # Every wrapper is called as +wrapper.call(ctx, base)+; a combine's as +wrapper.call(inputs, base)+
  # with the inputs and base HandlerBinder.unstage splits out.
  module HandlerBinder
    Candidate = Struct.new(:name, :method, :void)
    HandlerSet = Struct.new(:workflow, :target, :by_name)
    Binding = Struct.new(:activity, :step, :queue, :handler)
    Result = Struct.new(:bindings, :unserved)

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

    # Inventories +target+'s public step methods (those it adds over Object) by canonical step name.
    # Raises when two fold to the same name -- names differing only in case/style are ambiguous.
    def scan(workflow, target)
      by_name = {}
      (target.public_methods - Object.public_instance_methods).sort.each do |m|
        next if m.end_with?("=")

        method = target.method(m)
        next if method.parameters.any? { |type, _| type == :keyreq }

        step = ForFlow.step_name_for(target.class, m)
        canon = canonical(step)
        next if canon.empty?

        if by_name.key?(canon)
          raise ArgumentError, "handler methods '#{by_name[canon].name}' and '#{m}' both map to the same step " \
                               "name '#{canon}'; names differing only in case/style are ambiguous -- rename one"
        end
        by_name[canon] = Candidate.new(m.to_s, method, ForFlow.void?(target.class, m))
      end
      raise ArgumentError, "#{target.class} has no public handler methods" if by_name.empty?

      HandlerSet.new(workflow, target, by_name)
    end

    # Resolves +set+ against +graph+ (a registered definition Hash): each candidate is matched to a
    # worker-dispatched step by canonical name and wrapped per the node's kind. A method matching no
    # step, or a shape that clashes with the node, fails fast. Pure: no I/O.
    def bind(set, graph)
      workflow = set.workflow
      nodes = graph.fetch("nodes", []).select { |n| %w[TASK PREDICATE].include?(n["kind"]) && n["name"] }
      by_canon = {}
      nodes.each { |n| by_canon[canonical(n["name"])] ||= n }

      bindings = []
      served = Set.new
      set.by_name.each do |canon, cand|
        node = by_canon[canon]
        if node.nil?
          raise ArgumentError, "handler '#{cand.name}' matches no step in workflow '#{workflow}' " \
                               "(available steps: #{nodes.map { |n| n['name'] }.sort.inspect})"
        end
        activity = "#{workflow}##{node['name']}"
        queue = node.fetch("queue", workflow)
        wrappers(cand, node, activity).each do |suffix, handler|
          bindings << Binding.new(activity + suffix, node["name"], queue, handler)
        end
        served << node["name"]
      end
      Result.new(bindings, (nodes.map { |n| n["name"] } - served.to_a).sort)
    end

    # Whether a registered node is a fork or forEach combine: a TASK carrying armNames or a collectKey.
    def combine?(node)
      node["kind"] == "TASK" && (present?(node["armNames"]) || present?(node["collectKey"]))
    end

    # Splits a combine activation's context into the combine's inputs -- each fork arm's result in
    # arm order, or a forEach's collected results -- and the base (the context without the staged
    # results): +[inputs, base]+, or +[nil, nil]+ for a step that is not a combine. The engine stages
    # them under reserved keys, so they never collide with the workflow's own data.
    def unstage(ctx, arm_names, collect_key)
      return [nil, nil] if arm_names.empty? && !present?(collect_key)

      base = ctx.is_a?(Hash) ? ctx.dup : {}
      inputs = if arm_names.empty?
                 base.delete(STEPS_SCRATCH_PREFIX + collect_key)
                 [base.delete(collect_key)]
               else
                 arm_names.map { |arm| base.delete(ARM_SCRATCH_PREFIX + arm) }
               end
      [inputs, base]
    end

    # Invokes an undo with both of its snapshots: the engine stages them as the activation context
    # {"input" => ..., "result" => ...}. The return is nil -- an undo never changes the context.
    def compensation_wrapper(activity)
      lambda do |ctx, _base|
        ctx = {} unless ctx.is_a?(Hash)
        activity.compensate(Compensation.new(ctx["input"], ctx["result"]))
        nil
      end
    end

    # The [activity-suffix, wrapper] pairs a candidate serves on +node+.
    def wrappers(cand, node, activity)
      m = cand.method
      factory = m.parameters.empty?
      return [["", gate_wrapper(cand, activity)]] if node["kind"] == "PREDICATE"
      return [["", combine_wrapper(cand, node, activity)]] if combine?(node)

      if node["compensable"]
        inst = factory ? m.call : nil
        unless inst.is_a?(CompensableActivity)
          raise ArgumentError, "activity '#{activity}' is compensable; serve it with a zero-argument factory " \
                               "'#{cand.name}' returning a Wiggle::CompensableActivity (execute + compensate)"
        end
        return [["", ->(ctx, _base) { inst.execute(ctx) }], ["#compensate", compensation_wrapper(inst)]]
      end
      if cand.name.end_with?("?")
        raise ArgumentError, "activity '#{activity}' is a TASK but handler '#{cand.name}' is named like a " \
                             "predicate; that looks like a gate -- did you mean then_filter?"
      end
      return [["", task_from_factory(m.call, cand, activity)]] if factory

      require_arity!(cand, 1, activity)
      if cand.void
        [["", lambda { |ctx, _base|
          m.call(ctx)
          nil
        }]]
      else
        [["", ->(ctx, _base) { m.call(ctx) }]] # the return REPLACES the context (nil leaves it untouched)
      end
    end

    def gate_wrapper(cand, activity)
      m = cand.method
      if cand.void
        raise ArgumentError, "activity '#{activity}' is a gate (PREDICATE) but handler '#{cand.name}' is void; " \
                             "a gate returns whether the flow continues"
      end
      if m.parameters.empty?
        inst = m.call
        unless inst.is_a?(GateActivity)
          raise ArgumentError, "activity '#{activity}' is a gate (PREDICATE); factory '#{cand.name}' must return a " \
                               "Wiggle::GateActivity, not #{inst.class}"
        end
        return ->(ctx, _base) { inst.test(ctx) ? true : false }
      end
      require_arity!(cand, 1, activity)
      ->(ctx, _base) { m.call(ctx) ? true : false }
    end

    def task_from_factory(inst, cand, activity)
      case inst
      when CompensableActivity
        raise ArgumentError, "factory '#{cand.name}' returns a CompensableActivity, but activity '#{activity}' is " \
                             "not declared compensable (then_apply_compensable) -- its undo would never run"
      when Activity then ->(ctx, _base) { inst.execute(ctx) }
      when EffectActivity
        lambda { |ctx, _base|
          inst.apply(ctx)
          nil
        }
      else
        raise ArgumentError, "activity '#{activity}' is a TASK; factory '#{cand.name}' must return a Wiggle::Activity " \
                             "or Wiggle::EffectActivity, not #{inst.class}"
      end
    end

    # A combine takes its inputs positionally: each fork arm's result in arm order (or a forEach's
    # collected results), then optionally the frozen base.
    def combine_wrapper(cand, node, activity)
      m = cand.method
      if cand.void || m.parameters.empty?
        raise ArgumentError, "activity '#{activity}' is a combine; handler '#{cand.name}' takes the results to " \
                             "combine and must return the complete post-join context"
      end
      inputs = present?(node["armNames"]) ? node["armNames"] : [node["name"]]
      takes_base = accepts?(m, inputs.size + 1) && m.parameters.none? { |type, _| type == :rest }
      unless takes_base || accepts?(m, inputs.size)
        what = present?(node["armNames"]) ? "one per arm: #{inputs.join(', ')}" : "the collected results"
        raise ArgumentError, "combine '#{cand.name}' of activity '#{activity}' must take #{inputs.size} argument(s) " \
                             "(#{what}), optionally followed by the base context"
      end
      lambda do |args, base|
        takes_base ? m.call(*args, base) : m.call(*args)
      end
    end

    def require_arity!(cand, n, activity)
      return if accepts?(cand.method, n)

      raise ArgumentError, "handler '#{cand.name}' for activity '#{activity}' must take #{n} argument (the context) " \
                           "or none (a factory returning an Activity)"
    end

    # Whether +m+ can be called with exactly +n+ positional arguments.
    def accepts?(m, n)
      types = m.parameters.map(&:first)
      required = types.count(:req)
      optional = types.count(:opt)
      n >= required && (n <= required + optional || types.include?(:rest))
    end

    def present?(v) = !(v.nil? || (v.respond_to?(:empty?) && v.empty?))
  end
end
