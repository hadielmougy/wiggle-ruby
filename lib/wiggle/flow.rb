# frozen_string_literal: true

require "set"

module Wiggle
  # A step named by reference: what a contract stand-in (the +s+ in FlowSpec.define's block) hands
  # back for each of its methods.
  StepRef = Struct.new(:name)

  # Reads the node name off a step reference. This is the whole bridge between the WiggleFlow chain
  # and the name-keyed topology the server and worker speak: +s.withdraw+ (or +account.method(:withdraw)+)
  # yields "withdraw", emitted as the node name and later bound back to a handler method by the
  # worker's canonical folding (so +in_stock?+ and a node named +in-stock+ are the same step).
  #
  # A trailing +?+ or +!+ is not part of the node name. A method annotated with ForFlow#handles takes
  # that name instead.
  module StepNames
    module_function

    # The node name +ref+ stands for. +what+ names the call, for errors.
    def of(ref, what = "a flow step")
      case ref
      when StepRef then ref.name
      when Method, UnboundMethod then ForFlow.step_name_for(ref.owner, ref.name)
      when Symbol then plain(ref)
      when Proc
        raise ArgumentError, "#{what} must be a method reference (s.withdraw, account.method(:withdraw)), not a " \
                             "block or lambda: a block body has no handler method for a worker to bind"
      else
        raise ArgumentError, "#{what} must be a method reference (s.withdraw, account.method(:withdraw)); " \
                             "got #{ref.inspect}"
      end
    end

    # A method name as a node name: without a trailing ? or !.
    def plain(method_name) = method_name.to_s.sub(/[?!]\z/, "")
  end

  # The inert stand-in a contract module is turned into: every method the contract declares returns
  # a StepRef naming it, and nothing is ever invoked. A spec never runs a step -- it records the
  # step's name, and a worker supplies the code by matching that name to a method on its handler
  # object. A handler should +include+ the same contract, so the names cannot drift.
  module Steps
    module_function

    def of(contract)
      raise TypeError, "a contract is a Module declaring the steps; got #{contract.inspect}" unless contract.is_a?(Module)

      stand_in = Object.new
      names = contract.public_instance_methods(true) - Object.public_instance_methods
      raise ArgumentError, "contract #{contract} declares no steps" if names.empty?

      names.each do |m|
        ref = StepRef.new(StepNames.of(contract.instance_method(m))).freeze
        stand_in.define_singleton_method(m) { |*| ref }
      end
      stand_in.define_singleton_method(:inspect) { "#<steps of #{contract}>" }
      stand_in.freeze
    end
  end

  # Annotations for handler classes and contract modules -- Ruby's rendering of the Java client's
  # @ForFlow and @Handles, plus +void+ for a method that returns nothing (an effect).
  #
  #   module OrderSteps
  #     extend Wiggle::ForFlow
  #     def validate(order) = nil
  #     def in_stock?(order) = nil
  #     void def notify_customer(order) = nil
  #   end
  #
  #   class OrderHandlers
  #     extend Wiggle::ForFlow
  #     include OrderSteps
  #     for_flow "order-fulfilment"
  #
  #     def validate(order) = order.merge("status" => "VALIDATED")
  #     ...
  #   end
  module ForFlow
    # The workflow these handlers serve (Java's @ForFlow("...")).
    def for_flow(name = nil)
      return @__wiggle_for_flow = name.to_s unless name.nil?

      ForFlow.flow_name(self)
    end

    # Serve the step +step_name+ with +method_name+ (Java's @Handles("...")). Returns +method_name+,
    # so it can wrap a def: +handles "capture-payment", def do_capture(o) ... end+.
    def handles(step_name, method_name)
      (@__wiggle_handles ||= {})[method_name.to_sym] = step_name.to_s
      method_name
    end

    # Mark methods as returning nothing: effects, whose return is ignored and the context left
    # unchanged. Returns its argument, so +void def notify(o) ... end+ works.
    def void(*method_names)
      (@__wiggle_void ||= Set.new).merge(method_names.flatten.map(&:to_sym))
      method_names.size == 1 ? method_names.first : method_names
    end

    class << self
      # The @ForFlow name declared on +klass+ or an ancestor.
      def flow_name(klass)
        klass.ancestors.each do |a|
          name = a.instance_variable_get(:@__wiggle_for_flow)
          return name if name
        end
        nil
      end

      # The node name a method serves: its @Handles name, else the method name.
      def step_name_for(owner, method_name)
        owner.ancestors.each do |a|
          name = a.instance_variable_get(:@__wiggle_handles)&.fetch(method_name.to_sym, nil)
          return name if name
        end
        StepNames.plain(method_name)
      end

      # Whether +method_name+ is declared void on +klass+ or anything it includes.
      def void?(klass, method_name)
        klass.ancestors.any? { |a| a.instance_variable_get(:@__wiggle_void)&.include?(method_name.to_sym) }
      end
    end
  end

  # What a flow definition records before it becomes a graph. Each +then_*+ call on a WiggleFlow
  # appends a step to this tree, and #compile walks the tree once at the end.
  #
  # The indirection exists for one reason: Wiggle.all_of takes handles that already exist, so a
  # handle must be continuable more than once -- the second continuation is what makes a fan-out.
  # Recording first, compiling last, lets the fork be discovered after both of its arms have been
  # described. A step with two unclaimed children is a fan-out that was never combined, and #compile
  # rejects it by name.
  # @api private
  module Plan
    # One recorded operation (+op+ appends Topology nodes to an Array), and the steps after it.
    class Step
      attr_reader :parent, :label, :children
      attr_accessor :claimed

      def initialize(parent, op, label)
        @parent = parent
        @op = op
        @label = label
        @children = []
        @claimed = false
        parent&.children&.push(self)
      end

      def apply(out) = @op&.call(out)
    end

    # The step a Wiggle.all_of records: the arms it fans out into, and their merge.
    class Fork < Step
      attr_reader :arm_names, :combine_name

      def initialize(parent, arms, arm_names)
        super(parent, nil, nil)
        @arms = arms
        @arm_names = arm_names
      end

      # A fan-out joins exactly once; a second combine on the same stage is a mistake.
      def combine(name, retry_policy, queue)
        if @combine_name
          raise ArgumentError, "all_of(#{@arm_names.join(', ')}) already has a merge ('#{@combine_name}'); " \
                               "a fan-out joins once"
        end
        @combine_name = name
        @combine = Topology::Combine.new(name, retry_policy, queue)
      end

      def apply(out)
        raise ArgumentError, "all_of(#{@arm_names.join(', ')}) has no merge: follow it with combine(...)" unless @combine

        arms = @arms.each_with_index.map { |arm, i| [@arm_names[i], Plan.replay(arm, "arm '#{@arm_names[i]}'")] }
        out << Topology::Fork.new(arms, @combine)
      end
    end

    # The marker WiggleFlow#when / #otherwise records at the head of a Wiggle.one_of arm. It adds no
    # node of its own -- a choice builds its guards itself, in arm order.
    class Guard < Step
      attr_reader :gate

      def initialize(parent, gate)
        super(parent, nil, nil)
        @gate = gate # a Topology::Gate, or nil for otherwise
      end

      def fallback? = @gate.nil?
    end

    # The step a Wiggle.one_of records: an exclusive choice over its arms.
    class Choice < Step
      def initialize(parent, cases)
        super(parent, nil, nil)
        @cases = cases
      end

      def apply(out)
        out << Topology::Choose.new(@cases.map { |guard, arm, what| [guard, Plan.replay(arm, what)] })
      end
    end

    module_function

    def root = Step.new(nil, nil, nil)

    # Records a fan-out over +leaves+: finds the step they all branched from, lifts the path to each
    # out of the trunk, and hangs a Fork where they diverged.
    def fork(leaves)
      raise ArgumentError, "all_of needs at least two branches to fan out over" if leaves.size < 2

      junction = junction(leaves)
      arms = leaves.map { |leaf| claim(path_from(junction, leaf)) }
      names = arms.map { |arm| arm_name(arm) }
      if names.uniq.size != names.size
        raise ArgumentError, "all_of arms must be distinguishable, but two of them end at steps with the same name " \
                             "#{names}. Step names are unique, so this means an arm ends at a sleep -- give those " \
                             "sleeps different names with then_sleep(name, duration)."
      end
      Fork.new(junction, arms, names)
    end

    # Records an exclusive choice over +leaves+ -- the same junction and claiming as #fork, but each
    # arm must open with a WiggleFlow#when or #otherwise marker.
    def choice(leaves)
      raise ArgumentError, "one_of needs at least one branch to choose" if leaves.empty?

      junction = leaves.size == 1 ? sole_arm_junction(leaves.first) : junction(leaves)
      cases = leaves.map do |leaf|
        path = claim(path_from(junction, leaf))
        guard = path.first
        unless guard.is_a?(Guard)
          raise ArgumentError, "every arm of one_of must open with when(...) or otherwise: the arm ending at " \
                               "#{describe(leaf)} starts with #{describe(guard)}"
        end
        what = guard.fallback? ? "the otherwise arm" : "the '#{guard.gate.name}' arm"
        body = path.drop(1)
        raise ArgumentError, "#{what} of one_of has no steps" if body.empty?

        [guard.gate, body, what]
      end
      fallbacks = cases.each_index.select { |i| cases[i][0].nil? }
      raise ArgumentError, "one_of takes at most one otherwise arm" if fallbacks.size > 1
      raise ArgumentError, "the otherwise arm must be the last one given to one_of" if fallbacks.any? && fallbacks != [cases.size - 1]
      raise ArgumentError, "one_of needs at least one when(...) arm" if fallbacks.size == cases.size

      Choice.new(junction, cases)
    end

    # Walks the recorded tree once into Topology nodes and compiles them.
    def compile(name, version, default_retry, root)
      Topology.compile(name, version, default_retry, walk(root, [], "workflow '#{name}'"))
    end

    # Appends +from+'s descendants, following the single trunk at each step.
    def walk(from, out, what)
      step = from
      while (nxt = only_child(step, what))
        nxt.apply(out)
        step = nxt
      end
      out
    end

    # Replays one arm's recorded path into its own node list.
    def replay(arm, what)
      out = []
      arm.each { |step| step.apply(out) }
      last = arm.last
      raise ArgumentError, branch_error(last, what) if unclaimed(last).size > 1

      walk(last, out, what) # a single continuation after the arm's end is a step added to the arm afterwards
    end

    # A nested body (escalation, loop, forEach): runs the block once on a fresh root and returns a
    # lambda producing its node list.
    def body(what, &blk)
      raise ArgumentError, "#{what} needs a block" unless blk

      root = self.root
      tail = blk.call(WiggleFlow.new(root))
      raise ArgumentError, "#{what} must return the handle it ends on" unless tail.is_a?(WiggleFlow)

      -> { walk(root, [], what) }
    end

    def claim(path)
      path.each { |s| s.claimed = true }
      path
    end

    # What an arm is called: the name of the last step in it that has one (or a nested fan-out's
    # combine). The engine keys this arm's result by that name.
    def arm_name(arm)
      arm.reverse_each do |s|
        return s.label if s.label
        return s.combine_name if s.is_a?(Fork) && s.combine_name
      end
      raise ArgumentError, "an all_of arm has no step to take its name from -- it must contain at least one step, " \
                           "gate, combine or named sleep, since the engine keys the arm's result by that name"
    end

    # The nearest step every leaf descends from -- where the fork belongs.
    def junction(leaves)
      ancestors = Set.new
      s = leaves.first.parent
      while s
        ancestors << s
        s = s.parent
      end
      junction = nil
      leaves.drop(1).each do |leaf|
        s = leaf.parent
        s = s.parent while s && !ancestors.include?(s)
        if s.nil?
          raise ArgumentError, "all_of was given handles from different definitions (or from inside a branch it " \
                               "does not enclose); every arm must fan out from one common point"
        end
        junction = s if junction.nil? || ancestor?(junction, s)
      end
      raise ArgumentError, "all_of was given a handle that is an ancestor of another; arms must be siblings" if junction == leaves.first

      junction
    end

    def sole_arm_junction(leaf)
      s = leaf
      while s
        return s.parent if s.is_a?(Guard)

        s = s.parent
      end
      raise ArgumentError, "every arm of one_of must open with when(...) or otherwise: the arm ending at " \
                           "#{describe(leaf)} has no guard"
    end

    def ancestor?(maybe, step)
      s = step
      while s
        return true if s.equal?(maybe)

        s = s.parent
      end
      false
    end

    # The steps from just below +junction+ down to +leaf+, in order.
    def path_from(junction, leaf)
      path = []
      s = leaf
      until s.equal?(junction)
        raise ArgumentError, "all_of/one_of was given a handle that does not descend from the others' junction" if s.nil?
        raise ArgumentError, "the branch ending at #{describe(s)} is already an arm of another all_of/one_of" if s.claimed

        path << s
        s = s.parent
      end
      path.reverse
    end

    def only_child(step, what)
      nxt = unclaimed(step)
      raise ArgumentError, branch_error(step, what) if nxt.size > 1

      nxt.first
    end

    def unclaimed(step) = step.children.reject(&:claimed)

    def branch_error(step, what)
      heads = unclaimed(step).map { |c| describe(c) }
      "in #{what}, the flow after #{describe(step)} splits into [#{heads.join(', ')}] but is never rejoined. " \
        "Continuing one handle twice is a fan-out: pass both to Wiggle.all_of(...) and combine them, or continue " \
        "only one of them."
    end

    def describe(step)
      return "'#{step.label}'" if step.label
      return "when('#{step.gate.name}')" if step.is_a?(Guard) && !step.fallback?
      return "otherwise" if step.is_a?(Guard)

      step.parent.nil? ? "the start of the flow" : "a fork"
    end
  end

  # A handle on the point the flow has reached while it is being DEFINED -- shaped like the Java
  # client's WiggleFlow (itself shaped like CompletableFuture) so a workflow reads as a chain, but
  # nothing executes here. Each +then_*+ call records a step and returns a handle on the new end;
  # FlowSpec.define walks the recording once and compiles it.
  #
  #   FlowSpec.define("order-fulfilment", 1, OrderSteps) do |f, s|
  #     validated = f.then_apply(s.validate).then_filter(s.in_stock?)
  #
  #     payment  = validated.then_apply(s.charge)
  #     shipping = validated.then_apply(s.reserve).then_sleep(2).then_apply(s.label)
  #
  #     Wiggle.all_of(payment, shipping).combine(s.settle).then_accept(s.notify_customer)
  #   end
  #
  # Continuing a handle twice is a fan-out: +validated+ above is continued twice, and that is what
  # makes the two arms. They must be rejoined with Wiggle.all_of(...).combine(...) -- a split that is
  # never combined is rejected when the workflow is defined, naming the ends left dangling.
  #
  # Ruby control flow is unrolled: the block runs once, so a loop around +then_apply+ records that
  # many steps. Anything that depends on a step's RESULT must be Wiggle.one_of or #repeat_while,
  # which the engine evaluates at run time.
  #
  # Every step takes an optional RetryPolicy and an optional queue (a String), in either order. A step
  # with no policy inherits the workflow default given to FlowSpec.define; a step with no queue uses
  # the workflow's #default_queue (else the workflow name). Durations are seconds.
  class WiggleFlow
    # @api private
    attr_reader :step

    # @api private
    def initialize(step)
      @step = step
    end

    # A task step: the handler's return value becomes the new context.
    def then_apply(step_ref, *options)
      name = StepNames.of(step_ref)
      retry_policy, queue = WiggleFlow.options(options)
      record(name) { |out| out << Topology::Task.new(name, retry_policy, queue, false) }
    end
    alias apply then_apply

    # A task step whose handler carries its own undo: the handler side is a zero-argument factory
    # returning a Wiggle::CompensableActivity. The engine captures the step's input and result when it
    # completes, and runs +compensate+ in the reverse pass if the instance later fails.
    def then_apply_compensable(factory_ref, *options)
      name = StepNames.of(factory_ref)
      retry_policy, queue = WiggleFlow.options(options)
      record(name) { |out| out << Topology::Task.new(name, retry_policy, queue, true) }
    end
    alias apply_compensable then_apply_compensable

    # An effect step: the handler returns nothing (+void+), so the context is unchanged.
    def then_accept(step_ref, *options)
      name = StepNames.of(step_ref)
      retry_policy, queue = WiggleFlow.options(options)
      record(name) { |out| out << Topology::Task.new(name, retry_policy, queue, false) }
    end

    # A guard: the flow continues only while the handler holds. When it does not, the instance ends
    # (or, inside a branch, short-circuits to that branch's join).
    def then_filter(gate_ref, *options)
      name = StepNames.of(gate_ref)
      retry_policy, queue = WiggleFlow.options(options)
      record(name) { |out| out << Topology::Gate.new(name, retry_policy, queue) }
    end

    # A server-side timer, +duration+ seconds; no worker is held while the instance waits.
    # +then_sleep(duration)+ or +then_sleep(name, duration)+ for a readable node name.
    def then_sleep(*args)
      name, duration = args.size == 1 ? [nil, args.first] : args
      raise ArgumentError, "then_sleep takes (duration) or (name, duration)" if args.empty? || args.size > 2
      raise ArgumentError, "sleep duration must not be negative" if duration.negative?

      millis = (duration * 1000).round
      name ||= "sleep-#{millis}ms"
      record(name) { |out| out << Topology::Sleep.new(name, millis) }
    end

    # Waits for +signal+ from an external actor; its payload merges into the context. With a
    # +timeout+ (seconds) the instance fails if it does not arrive in time -- unless a block is given:
    # the escalation branch that runs instead, rejoining the flow afterwards.
    #
    #   f.then_await("approval", 3600) { |esc| esc.then_apply(s.auto_approve) }
    def then_await(signal, timeout = nil, &escalation)
      raise ArgumentError, "timeout must not be negative" if timeout&.negative?
      raise ArgumentError, "escalation needs a timeout" if escalation && timeout.nil?

      millis = timeout.nil? ? 0 : (timeout * 1000).round
      esc = escalation && Plan.body("the escalation branch of '#{signal}'", &escalation)
      record(signal) { |out| out << Topology::Signal.new(signal, millis, esc&.call) }
    end

    # Runs another registered workflow (a name or FlowSpec) as a child instance at node +node+,
    # starting from this context; its final context merges back here.
    def then_sub_flow(node, workflow)
      child = workflow.is_a?(FlowSpec) ? workflow.name : workflow.to_s
      raise ArgumentError, "then_sub_flow needs a child workflow name" if child.empty?

      record(node) { |out| out << Topology::SubWorkflow.new(node, child) }
    end

    # Runtime fan-out: when the instance reaches this node the engine reads the collection stored in
    # the context under +items_key+ and runs the body once per element, in parallel, EACH ELEMENT BEING
    # THAT BRANCH'S WHOLE CONTEXT. Its Items#combine receives every item's final value and returns the
    # complete post-join context. +then_for_each(items_key) { |item| ... }+, or
    # +then_for_each(name, items_key) { ... }+ when the same key is fanned over twice.
    def then_for_each(*args, &loop_body)
      raise ArgumentError, "then_for_each takes (items_key) or (name, items_key)" unless [1, 2].include?(args.size)

      name, items_key = args.size == 1 ? [args[0], args[0]] : args
      Items.new(self, name.to_s, items_key.to_s, Plan.body("the forEach body for '#{name}'", &loop_body))
    end

    # Opens an arm of a Wiggle.one_of: the steps chained after this run only when +guard+ is the first
    # of the choice's guards to hold. Not #then_filter: a gate that fails ends the instance, while a
    # guard that fails hands the choice to the next arm.
    def when(guard_ref, *options)
      name = StepNames.of(guard_ref, "a when(...) guard")
      retry_policy, queue = WiggleFlow.options(options)
      WiggleFlow.new(Plan::Guard.new(@step, Topology::Gate.new(name, retry_policy, queue)))
    end

    # Opens the default arm of a Wiggle.one_of: it runs when none of the guards held. It must be the
    # last arm given to +one_of+.
    def otherwise = WiggleFlow.new(Plan::Guard.new(@step, nil))

    # A do-while loop: the body runs once, then +condition+ is evaluated on a worker, and while it
    # holds the body runs again (the body always runs at least once). Every loop is budgeted: pass
    # +max_iterations+ (an Integer) for more than the engine default; a String pins the condition to
    # a queue.
    #
    #   f.repeat_while(s.more?, 50) { |body| body.then_apply(s.fetch_page) }
    def repeat_while(condition_ref, *options, &loop_body)
      name = StepNames.of(condition_ref)
      max_iterations = options.find { |o| o.is_a?(Integer) } || -1
      queue = options.find { |o| o.is_a?(String) }
      unknown = options.reject { |o| o.is_a?(Integer) || o.is_a?(String) }
      raise ArgumentError, "repeat_while takes an Integer max_iterations and/or a String queue; got #{unknown.inspect}" if unknown.any?
      raise ArgumentError, "max_iterations must be positive" if max_iterations != -1 && max_iterations < 1

      body = Plan.body("the body of loop '#{name}'", &loop_body)
      record(name) { |out| out << Topology::Loop.new(name, max_iterations, body.call, queue) }
    end

    # Sets the queue used by every step defined after this point.
    def default_queue(queue)
      record(nil) { |out| out << Topology::DefaultQueue.new(queue.to_s) }
    end

    # @api private -- Items records the forEach once its combine is known.
    def record_for_each(name, items_key, body, combine)
      record(name) { |out| out << Topology::ForEach.new(name, items_key, body.call, combine) }
    end

    # @api private -- splits Java-style trailing (retry, queue) arguments, in either order.
    def self.options(options)
      retry_policy = nil
      queue = nil
      options.each do |o|
        case o
        when RetryPolicy
          raise ArgumentError, "a step takes at most one RetryPolicy" if retry_policy

          retry_policy = o
        when String
          raise ArgumentError, "a step takes at most one queue" if queue

          queue = o
        else raise ArgumentError, "a step's options are a RetryPolicy and/or a queue name; got #{o.inspect}"
        end
      end
      [retry_policy, queue]
    end

    def inspect = "#<Wiggle::WiggleFlow at #{Plan.describe(@step)}>"

    private

    def record(label, &op) = WiggleFlow.new(Plan::Step.new(@step, op, label))
  end

  # The stage WiggleFlow#then_for_each returns; its #combine is mandatory. Each item ran on its own
  # isolated context -- the element itself -- so a combine is the only way the results reach the
  # flow. They arrive as an Array ordered by item index, or a Hash keyed like the input.
  class Items
    # @api private
    def initialize(from, name, items_key, body)
      @from = from
      @name = name
      @items_key = items_key
      @body = body
    end

    # The merge: a step reference (or a name), plus an optional RetryPolicy and/or queue.
    def combine(step_ref, *options)
      name = step_ref.is_a?(String) ? step_ref : StepNames.of(step_ref, "a combine")
      retry_policy, queue = WiggleFlow.options(options)
      @from.record_for_each(@name, @items_key, @body, Topology::Combine.new(name, retry_policy, queue))
    end
  end

  # The stage a Wiggle.all_of returns; its #combine is mandatory. The combine handler receives one
  # argument per arm, in the order the arms were given, and returns the complete post-join context.
  class Fork
    # @api private
    def initialize(fork)
      @fork = fork
    end

    # The merge: a step reference (or a name), plus an optional RetryPolicy and/or queue.
    def combine(step_ref, *options)
      name = step_ref.is_a?(String) ? step_ref : StepNames.of(step_ref, "a combine")
      retry_policy, queue = WiggleFlow.options(options)
      @fork.combine(name, retry_policy, queue)
      WiggleFlow.new(@fork)
    end
  end

  # Fans the flow out over handles that branched from a common point, and returns the mandatory
  # combine stage. Each arm runs on its OWN ISOLATED COPY of the context -- an arm's writes never
  # touch the shared context -- so the combine is the only way an arm's result reaches the flow. An
  # arm is named after its last step, which is how the engine keys its result for the combine.
  def self.all_of(*arms)
    Fork.new(Plan.fork(handles(arms, "all_of")))
  end

  # all_of's exclusive twin: of the arms given, EXACTLY ONE runs -- the first whose guard holds, or
  # the +otherwise+ arm if none did. Every arm must open with WiggleFlow#when or #otherwise. No
  # combine: the arms are alternatives on the one context, so control continues from whichever ran.
  #
  #   vip      = f.when(s.vip?).then_apply(s.vip_path)
  #   standard = f.otherwise.then_apply(s.standard_path)
  #   Wiggle.one_of(vip, standard)
  def self.one_of(*arms)
    WiggleFlow.new(Plan.choice(handles(arms, "one_of")))
  end

  def self.handles(arms, what)
    arms.flatten.map do |a|
      raise ArgumentError, "#{what} takes WiggleFlow handles; got #{a.inspect}" unless a.is_a?(WiggleFlow)

      a.step
    end
  end
  private_class_method :handles

  # An immutable workflow topology (nodes, edges, kinds, queues, retry) to register with the server.
  # It carries no step logic: step implementations are bound by name on a Worker.
  class FlowSpec
    attr_reader :definition

    # @api private -- build one with FlowSpec.define.
    def initialize(definition)
      @definition = definition.freeze
    end

    def name = @definition["name"]

    def version = @definition["version"]

    def queues = @definition["queues"]

    def ==(other) = other.is_a?(FlowSpec) && other.definition == definition
    alias eql? ==

    def hash = definition.hash

    # Defines a workflow. +version+ is declared, not derived: a published version is immutable, so a
    # changed graph needs a new number. Optional arguments, in any order: a RetryPolicy (the default
    # for every step that names none) and a contract Module declaring the steps.
    #
    # With a contract the block receives the start handle and an inert stand-in for the contract,
    # through which steps are named (+s.validate+); calling a method on it only names the step. A
    # handler class should +include+ the same contract, so the names cannot drift:
    #
    #   FlowSpec.define("order-fulfilment", 1, OrderSteps) do |f, s|
    #     f.then_apply(s.validate).then_filter(s.in_stock?)
    #   end
    #
    # Without one, the block receives only the start handle and names steps by method reference
    # (+handlers.method(:validate)+) or Symbol. Either way the block must return the handle it ends on.
    def self.define(name, version, *options, &body)
      raise ArgumentError, "workflow '#{name}' has no body" unless body

      default_retry = options.find { |o| o.is_a?(RetryPolicy) }
      contract = options.find { |o| o.is_a?(Module) }
      unknown = options - [default_retry, contract]
      raise ArgumentError, "FlowSpec.define takes a RetryPolicy and/or a contract Module; got #{unknown.inspect}" if unknown.any?

      root = Plan.root
      start = WiggleFlow.new(root)
      tail = contract ? body.call(start, Steps.of(contract)) : body.call(start)
      raise ArgumentError, "the body of workflow '#{name}' must return the handle it ends on" unless tail.is_a?(WiggleFlow)

      Plan.compile(name, version, default_retry, root)
    end
  end
end
