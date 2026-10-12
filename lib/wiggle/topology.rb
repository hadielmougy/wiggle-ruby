# frozen_string_literal: true

require "set"

module Wiggle
  # The engine stages a join's results under reserved keys so they cannot collide with the
  # workflow's own data: a fork arm under ARM_SCRATCH_PREFIX + arm, a forEach's collected results
  # under the combine's collectKey, and the step that produced each result under
  # STEPS_SCRATCH_PREFIX + collectKey. The worker unstages them before a combine handler runs.
  ARM_SCRATCH_PREFIX = "__arm__"
  FOR_EACH_SCRATCH_PREFIX = "__forEach__"
  STEPS_SCRATCH_PREFIX = "__steps__"

  # @api private
  #
  # The graph compiler behind FlowSpec.define. A recorded flow (see Plan) is replayed into a list of
  # these node values, which #compile turns into the definition the server stores: node ids, edges,
  # queues and retry policies. Users never build these directly -- they write a WiggleFlow chain.
  module Topology
    Task = Struct.new(:name, :retry, :queue, :compensable)
    Gate = Struct.new(:name, :retry, :queue)
    Sleep = Struct.new(:name, :millis)
    Signal = Struct.new(:name, :timeout_millis, :escalation)
    SubWorkflow = Struct.new(:name, :workflow)
    Combine = Struct.new(:name, :retry, :queue)
    Fork = Struct.new(:arms, :combine)               # arms: [[name, nodes]]
    ForEach = Struct.new(:name, :items_key, :body, :combine)
    Choose = Struct.new(:cases)                      # cases: [[Gate | nil (otherwise), nodes]]
    Loop = Struct.new(:condition, :max_iterations, :body, :queue)
    DefaultQueue = Struct.new(:queue)

    module_function

    def compile(name, version, default_retry, nodes)
      raise ArgumentError, "workflow name is required" if name.nil? || name.to_s.strip.empty?
      raise TypeError, "version must be an Integer" unless version.is_a?(Integer)
      raise ArgumentError, "version must be in 1..#{0x7FFFFFFF}" unless (1..0x7FFFFFFF).cover?(version)
      raise ArgumentError, "workflow '#{name}' has no steps" if nodes.empty?

      store = Store.new(name, default_retry || RetryPolicy.forever)
      root = Stream.new(store, nil, root: true)
      root.append_all(nodes)
      end_id = store.add_end
      root.wire_open_to(end_id)
      store.start_node ||= end_id

      queues = store.queues.to_a.sort
      definition = { "version" => version, "name" => name, "startNode" => store.start_node,
                     "nodes" => store.nodes.values, "queues" => queues, "executionMode" => "SERVER" }
      FlowSpec.new(definition)
    end

    # The accumulating node store shared by a workflow and all its nested streams.
    class Store
      attr_reader :nodes, :queues
      attr_accessor :start_node, :default_queue

      def initialize(name, default_retry)
        @name = name
        @default_retry = default_retry
        @default_queue = name
        @nodes = {}
        @queues = Set.new
        @reserved = Set.new
        @counter = 0
      end

      def add_worker(kind, name, retry_policy, queue)
        reserve(name)
        nid = next_id
        q = queue || @default_queue
        @queues << q
        @nodes[nid] = { "id" => nid, "kind" => kind, "name" => name, "activity" => "#{@name}##{name}",
                        "queue" => q, "retry" => (retry_policy || @default_retry).to_json_hash }
        nid
      end

      def add_timer(kind, name, millis, reserve:)
        reserve(name) if reserve
        nid = next_id
        node = { "id" => nid, "kind" => kind, "name" => name }
        node["sleepMillis"] = millis if millis.positive?
        @nodes[nid] = node
        nid
      end

      def add_fork
        nid = next_id("fork")
        @nodes[nid] = { "id" => nid, "kind" => "FORK", "name" => nid }
        nid
      end

      def add_subworkflow(name, child)
        reserve(name)
        nid = next_id("sub")
        # the child workflow's name travels in `activity`; the engine starts it (no worker handler)
        @nodes[nid] = { "id" => nid, "kind" => "SUB_WORKFLOW", "name" => name, "activity" => child }
        nid
      end

      def add_dynfork(name, items_key)
        reserve(name)
        nid = next_id("dynfork")
        @nodes[nid] = { "id" => nid, "kind" => "DYN_FORK", "name" => name, "itemsKey" => items_key, "itemKey" => items_key }
        nid
      end

      # The mandatory merge after a fork's join: a TASK bound by name like any step, carrying the
      # fork's armNames so the engine can stage each isolated branch's result for the handler.
      def add_fork_combine(combine, arms)
        nid = add_worker("TASK", combine.name, combine.retry, combine.queue)
        @nodes[nid]["armNames"] = arms
        nid
      end

      # The mandatory merge after a forEach's join: a TASK whose collectKey is the scratch key the
      # engine stages the collected item results under.
      def add_for_each_combine(combine, for_each_name)
        nid = add_worker("TASK", combine.name, combine.retry, combine.queue)
        @nodes[nid]["collectKey"] = FOR_EACH_SCRATCH_PREFIX + for_each_name
        nid
      end

      def add_join(expected)
        nid = next_id("join")
        node = { "id" => nid, "kind" => "JOIN", "name" => nid }
        node["expected"] = expected if expected.positive? # a dynamic join carries its width at run time
        @nodes[nid] = node
        nid
      end

      def add_end(reason = nil)
        nid = next_id("end")
        node = { "id" => nid, "kind" => "END", "success" => true }
        node["reason"] = reason unless reason.nil?
        @nodes[nid] = node
        nid
      end

      def wire(node_id, edge, target)
        @nodes[node_id][edge == :next ? "next" : "altNext"] = target
      end

      private

      def next_id(prefix = "n")
        @counter += 1
        "#{prefix}#{@counter}"
      end

      def reserve(name)
        raise ArgumentError, "a step is missing a name" if name.nil? || name.to_s.empty?
        raise ArgumentError, "duplicate step name '#{name}': node names must be unique in a workflow" if @reserved.include?(name)

        @reserved << name
      end
    end

    # Open-ends wiring over a node list. One per stream (the root, plus one per nested arm / case /
    # body / escalation); they all share a single Store.
    class Stream
      attr_reader :start, :open

      def initialize(store, enclosing_join, root: false)
        @s = store
        @enclosing_join = enclosing_join
        @root = root
        @open = [] # [node_id, :next | :alt] ends waiting to be wired
        @start = nil
      end

      def append_all(nodes) = nodes.each { |n| append(n) }

      def wire_open_to(target)
        @open.each { |nid, edge| @s.wire(nid, edge, target) }
        @open = []
      end

      private

      def sub(enclosing_join) = Stream.new(@s, enclosing_join)

      # Wire the open ends to +node_id+; the very first node becomes this stream's start.
      def attach(node_id)
        if @open.any?
          wire_open_to(node_id)
        elsif @start.nil?
          @start = node_id
          @s.start_node = node_id if @root
        end
      end

      def chain(node_id)
        attach(node_id)
        @open = [[node_id, :next]]
      end

      def append(n)
        case n
        when Task
          nid = @s.add_worker("TASK", n.name, n.retry, n.queue)
          @s.nodes[nid]["compensable"] = true if n.compensable
          chain(nid)
        when Gate
          nid = @s.add_worker("PREDICATE", n.name, n.retry, n.queue)
          attach(nid)
          @s.wire(nid, :alt, @enclosing_join || @s.add_end("gated:#{n.name}"))
          @open = [[nid, :next]]
        when Sleep then chain(@s.add_timer("SLEEP", n.name, n.millis, reserve: false))
        when Signal then append_signal(n)
        when SubWorkflow then chain(@s.add_subworkflow(n.name, n.workflow))
        when Fork then append_fork(n)
        when ForEach then append_for_each(n)
        when Choose then append_choose(n)
        when Loop then append_loop(n)
        when DefaultQueue then @s.default_queue = n.queue
        else raise TypeError, "not a topology node: #{n.inspect}"
        end
      end

      def append_signal(n)
        nid = @s.add_timer("SIGNAL", n.name, n.timeout_millis, reserve: true)
        attach(nid)
        if n.escalation.nil?
          @open = [[nid, :next]]
          return
        end
        # delivery path is `next`; the escalation branch hangs off `alt` and its tail rejoins, so both
        # continue to whatever follows
        esc = build(n.escalation, @enclosing_join, "the escalation branch of '#{n.name}'")
        @s.wire(nid, :alt, esc.start)
        @open = [[nid, :next], *esc.open]
      end

      def append_fork(n)
        fork_id = @s.add_fork
        attach(fork_id)
        join_id = @s.add_join(n.arms.size)
        starts = n.arms.map do |name, nodes|
          arm = build(nodes, join_id, "arm '#{name}'")
          arm.wire_open_to(join_id)
          arm.start
        end
        @s.nodes[fork_id]["branches"] = starts
        combine_id = @s.add_fork_combine(n.combine, n.arms.map(&:first))
        @s.wire(join_id, :next, combine_id)
        @open = [[combine_id, :next]]
      end

      def append_for_each(n)
        fork_id = @s.add_dynfork(n.name, n.items_key)
        attach(fork_id)
        join_id = @s.add_join(0) # 0 = dynamic width
        body = build(n.body, join_id, "the forEach body for '#{n.name}'")
        body.wire_open_to(join_id)
        @s.nodes[fork_id]["branches"] = [body.start]
        @s.wire(fork_id, :next, join_id) # empty-collection skip (past the combine, engine-side)
        combine_id = @s.add_for_each_combine(n.combine, n.name)
        @s.wire(join_id, :next, combine_id)
        @open = [[combine_id, :next]]
      end

      def append_loop(n)
        body = build(n.body, @enclosing_join, "the body of loop '#{n.condition}'")
        cond_id = @s.add_worker("PREDICATE", n.condition, nil, n.queue)
        @s.nodes[cond_id]["loopBudget"] = n.max_iterations # -1 = the engine default
        attach(body.start)                 # enter at the body
        body.wire_open_to(cond_id)         # body tail -> condition
        @s.wire(cond_id, :next, body.start) # true: loop back to the body
        @open = [[cond_id, :alt]]          # false: continue onward
      end

      def append_choose(n)
        guarded = n.cases.reject { |guard, _| guard.nil? }
        fallback = n.cases.find { |guard, _| guard.nil? }
        guard_ids = guarded.map { |guard, _| @s.add_worker("PREDICATE", guard.name, guard.retry, guard.queue) }
        attach(guard_ids.first)
        @open = []
        guard_ids.each_cons(2) { |a, b| @s.wire(a, :alt, b) } # each guard's false path -> the next guard
        guarded.each_with_index { |(guard, nodes), i| collect_case(nodes, guard_ids[i], :next, "the '#{guard.name}' arm") }
        if fallback
          collect_case(fallback[1], guard_ids.last, :alt, "the otherwise arm")
        else
          @open << [guard_ids.last, :alt] # nothing matched: skip past the choice
        end
      end

      def collect_case(nodes, guard_id, edge, what)
        arm = build(nodes, @enclosing_join, what)
        @s.wire(guard_id, edge, arm.start)
        @open.concat(arm.open)
      end

      def build(nodes, enclosing_join, what)
        stream = sub(enclosing_join)
        stream.append_all(nodes)
        raise ArgumentError, "#{what} defines no steps" if stream.start.nil?

        stream
      end
    end
  end
end
