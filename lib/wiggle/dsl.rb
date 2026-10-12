# frozen_string_literal: true

module Wiggle
  # Describe a workflow's topology with a block instead of nested node values. It builds exactly the
  # same Graph (and so the same definition and content-hash version) as the declarative form.
  #
  #   wf = Wiggle.workflow("order") do
  #     step "validate"
  #     gate "in-stock"
  #     fork combine: "merge" do
  #       branch("payment")  { step "charge", queue: "payments", retry: Wiggle::Retry.exponential(5, 0.1) }
  #       branch("shipping") { step "reserve"; step "label" }
  #     end
  #     choose do
  #       on("vip") { step "concierge" }
  #       otherwise { step "thanks" }
  #     end
  #     effect "notify"
  #   end
  #
  # Returns the compiled FlowSpec. Pass +compile: false+ to get the Graph instead. A block taking one
  # parameter is yielded the builder (so the outer +self+ stays reachable); otherwise it is
  # instance-evaluated.
  def self.workflow(name, version: nil, default_queue: nil, compile: true, &blk)
    graph = Graph.new(name, DSL.nodes(&blk), version: version, default_queue: default_queue)
    compile ? graph.compile : graph
  end

  # The node-list builder behind Wiggle.workflow; every nested block (branch, case, body,
  # escalation) gets its own.
  class DSL
    def self.nodes(&blk)
      raise ArgumentError, "a workflow block is required" unless blk

      dsl = new
      blk.arity == 1 ? blk.call(dsl) : dsl.instance_eval(&blk)
      dsl.result
    end

    attr_reader :result

    def initialize
      @result = []
    end

    def step(name, queue: nil, retry: nil, compensate: false)
      add Step.new(name, queue: queue, retry: binding.local_variable_get(:retry), compensate: compensate)
    end

    def effect(name, queue: nil, retry: nil, compensate: false)
      add Effect.new(name, queue: queue, retry: binding.local_variable_get(:retry), compensate: compensate)
    end

    def gate(name, queue: nil, retry: nil)
      add Gate.new(name, queue: queue, retry: binding.local_variable_get(:retry))
    end

    # A server-side timer (named +sleep+ after the node; Kernel#sleep is not reachable in this block).
    def sleep(name, seconds: 0, millis: 0)
      add Sleep.new(name, seconds: seconds, millis: millis)
    end

    # Wait for a signal; a block given here is the escalation run on timeout.
    def await_signal(name, timeout: 0, &escalation)
      add AwaitSignal.new(name, timeout: timeout, escalation: escalation && DSL.nodes(&escalation))
    end

    def sub_workflow(name, workflow)
      add SubWorkflow.new(name, workflow)
    end

    # Parallel branches declared with #branch inside the block, rejoined at +combine+.
    def fork(combine:, &blk)
      add Fork.new(BranchList.collect(&blk), combine: combine)
    end

    def for_each(name, over:, combine:, &body)
      add ForEach.new(name, over: over, body: DSL.nodes(&body), combine: combine)
    end

    # Exclusive choice declared with #on / #otherwise inside the block.
    def choose(&blk)
      add Choose.new(CaseList.collect(&blk))
    end

    def do_while(condition, max_iterations: 0, &body)
      add DoWhile.new(condition, DSL.nodes(&body), max_iterations: max_iterations)
    end

    # Splice already-built node values into the flow.
    def nodes(*list)
      list.flatten.each { |n| add(n) }
      self
    end

    private

    def add(node)
      @result << node
      self
    end

    # @api private
    class BranchList
      def self.collect(&blk)
        raise ArgumentError, "fork needs a block declaring its branches" unless blk

        list = new
        blk.arity == 1 ? blk.call(list) : list.instance_eval(&blk)
        list.branches
      end

      attr_reader :branches

      def initialize
        @branches = []
      end

      def branch(name, &blk)
        @branches << Branch.new(name, DSL.nodes(&blk))
        self
      end
    end

    # @api private
    class CaseList
      def self.collect(&blk)
        raise ArgumentError, "choose needs a block declaring its cases" unless blk

        list = new
        blk.arity == 1 ? blk.call(list) : list.instance_eval(&blk)
        list.cases
      end

      attr_reader :cases

      def initialize
        @cases = []
      end

      def on(guard, &blk)
        @cases << Case.on(guard, DSL.nodes(&blk))
        self
      end

      def otherwise(&blk)
        @cases << Case.otherwise(DSL.nodes(&blk))
        self
      end
    end
  end
end
