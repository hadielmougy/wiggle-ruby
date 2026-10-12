# frozen_string_literal: true

require "digest"
require "set"

module Wiggle
  # The workflow topology as declarative data: describe a graph of steps, gates, timers, signal waits,
  # parallel Fork and exclusive Choose as a Graph of node values that mirror the graph schema, then
  # Graph#compile it into a FlowSpec you register and start.
  #
  # Handlers are NOT part of the topology -- a Worker binds them separately, by name (+handle+ /
  # +handle_gate+ / +handle_effect+). The graph lives on the server; any worker that implements the
  # matching activity names can run its steps, so Ruby, Java, Python and Go workers interoperate.
  #
  #   wf = Wiggle::Graph.new("order", [
  #     Wiggle::Step.new("validate"),
  #     Wiggle::Gate.new("in-stock"),
  #     Wiggle::Fork.new([
  #       Wiggle::Branch.new("payment",  [Wiggle::Step.new("charge", queue: "payments")]),
  #       Wiggle::Branch.new("shipping", [Wiggle::Step.new("reserve"), Wiggle::Step.new("label")]),
  #     ], combine: "merge"),
  #     Wiggle::Effect.new("notify"),
  #   ]).compile
  #
  # or, equivalently, with the block builder (see Wiggle.workflow).

  MAX_INT = 2_147_483_647

  # The engine stages a join's results under reserved keys so they cannot collide with the
  # workflow's own data: a fork arm under ARM_SCRATCH_PREFIX + arm, a forEach's collected results
  # under the combine's collectKey, and the step that produced each result under
  # STEPS_SCRATCH_PREFIX + collectKey. The worker unstages them before a combine handler runs.
  ARM_SCRATCH_PREFIX = "__arm__"
  FOR_EACH_SCRATCH_PREFIX = "__forEach__"
  STEPS_SCRATCH_PREFIX = "__steps__"

  # A per-step retry policy. Backoffs are given in seconds.
  Retry = Struct.new(:max_attempts, :initial_backoff, :multiplier, :max_backoff, :jitter, keyword_init: true) do
    def initialize(max_attempts: MAX_INT, initial_backoff: 1.0, multiplier: 1.0, max_backoff: 60.0, jitter: 0.0)
      super
      freeze
    end

    def to_json_hash
      {
        "maxAttempts" => Integer(max_attempts),
        "initialBackoffMillis" => (initial_backoff * 1000).to_i,
        "multiplier" => multiplier.to_f,
        "maxBackoffMillis" => (max_backoff * 1000).to_i,
        "jitter" => jitter.to_f
      }
    end

    # Retry forever, 1s apart (the default when a step names no policy).
    def self.forever = new(max_attempts: MAX_INT, initial_backoff: 1.0, multiplier: 1.0, max_backoff: 60.0, jitter: 0.0)

    # A single attempt: no retries.
    def self.none = new(max_attempts: 1, initial_backoff: 0.0, multiplier: 1.0, max_backoff: 0.0, jitter: 0.0)

    # Doubling backoff from +initial_backoff+ seconds, capped at 5 minutes, with 20% jitter.
    def self.exponential(max_attempts, initial_backoff)
      new(max_attempts: max_attempts, initial_backoff: initial_backoff, multiplier: 2.0, max_backoff: 300.0, jitter: 0.2)
    end

    # A constant +backoff+ seconds between attempts.
    def self.fixed(max_attempts, backoff)
      new(max_attempts: max_attempts, initial_backoff: backoff, multiplier: 1.0, max_backoff: backoff, jitter: 0.0)
    end
  end

  # ---- the declarative node types (mirror the graph schema) ----

  # Marker for a topology node. A workflow's +steps+ is an Array of these.
  module Node; end

  # A unit of work run on a worker; its handler returns the new context. Bind it with Worker#handle.
  #
  # +compensate: true+ declares the step compensable: if the instance later fails, the engine runs the
  # step's compensator (bound with Worker#handle_compensation, or a +compensate_<step>+ method on a
  # handlers object) in the reverse pass, handing it the input/result context snapshots captured at
  # this step's completion.
  Step = Struct.new(:name, :queue, :retry, :compensate) do
    include Node

    def initialize(name, queue: nil, retry: nil, compensate: false) = super(name, queue, binding.local_variable_get(:retry), compensate)
  end

  # A step run for its side effect only; the context is unchanged. Bind it with Worker#handle_effect.
  # (Topologically identical to a Step.) +compensate+ declares an undo exactly as on a Step.
  Effect = Struct.new(:name, :queue, :retry, :compensate) do
    include Node

    def initialize(name, queue: nil, retry: nil, compensate: false) = super(name, queue, binding.local_variable_get(:retry), compensate)
  end

  # A predicate: continue only while it holds; a false result ends the instance as +gated:<name>+
  # (inside a fork/choose branch it short-circuits to the enclosing join). Bind it with
  # Worker#handle_gate.
  Gate = Struct.new(:name, :queue, :retry) do
    include Node

    def initialize(name, queue: nil, retry: nil) = super(name, queue, binding.local_variable_get(:retry))
  end

  # A server-side timer; no worker is held while the instance waits.
  Sleep = Struct.new(:name, :seconds, :millis) do
    include Node

    def initialize(name, seconds: 0, millis: 0) = super(name, seconds, millis)
  end

  # Wait for a named external signal (delivered via WiggleClient#signal); the payload merges into the
  # context like a step's result. With a positive +timeout+ (seconds) the instance FAILS if the signal
  # does not arrive in time -- unless an +escalation+ list is given, in which case those nodes run
  # instead on timeout and then rejoin the flow (exactly one of delivery / escalation happens).
  AwaitSignal = Struct.new(:name, :timeout, :escalation) do
    include Node

    def initialize(name, timeout: 0, escalation: nil) = super(name, timeout, escalation)
  end

  # Run another registered workflow as a child: it starts with this instance's context, and on
  # completion its final context merges back here. The child must be registered separately;
  # +workflow+ is its name (or a FlowSpec / Graph to take the name from).
  SubWorkflow = Struct.new(:name, :workflow) do
    include Node
  end

  # One parallel arm of a Fork (a name plus its own list of nodes).
  Branch = Struct.new(:name, :steps)

  # Fan out into parallel branches and wait for all of them (join). Needs >= 2 branches.
  #
  # +combine+ names the MANDATORY merge step run after the join: its handler receives the context with
  # each branch's result staged under the branch's name, and must return the COMPLETE post-join
  # context -- the engine replaces the context with it, so keys the handler omits do not survive the
  # join. There is no implicit fold of the arms. Bind it with Worker#handle_combine.
  Fork = Struct.new(:branches, :combine) do
    include Node

    def initialize(branches, combine: nil) = super(branches, combine)
  end

  # Runtime fan-out: at run time the engine reads the collection in the context at +over+ (a list or a
  # map) and spawns one ISOLATED parallel branch per element. The element IS the item's context: body
  # handlers receive the item's value (scalars included), their return replaces it, and the frozen
  # base rides on Wiggle::Current.base. +combine+ names the MANDATORY merge step: its handler receives
  # the context with every item's final value collected under +name+ -- a list ordered by item index
  # for a list input, a map keyed like the input for a map input -- and must return the COMPLETE
  # post-join context. An empty collection skips the body and the combine.
  ForEach = Struct.new(:name, :over, :body, :combine) do
    include Node

    def initialize(name, over:, body:, combine: nil) = super(name, over, body, combine)
  end

  # One arm of a Choose. +when_+ nil marks the +otherwise+ default (which must be last); otherwise it
  # is the name of the guard predicate.
  Case = Struct.new(:then_, :when_) do
    def initialize(then_, when_ = nil) = super

    # A guarded case: runs +steps+ when the predicate step +guard+ holds.
    def self.on(guard, steps) = new(steps, guard)

    # The default case (must be last).
    def self.otherwise(steps) = new(steps, nil)
  end

  # Exclusive choice: the first Case whose guard holds runs, the rest are skipped. A cascade of guards
  # -- nothing runs in parallel and there is no join.
  Choose = Struct.new(:cases) do
    include Node
  end

  # A do-while loop: run +body+ once, then evaluate the +condition+ predicate on a worker; while it
  # holds, the body runs again (the body always runs at least once).
  #
  # Every loop is budgeted: the guard may evaluate true at most +max_iterations+ times, after which the
  # instance FAILS with a clear error -- an unbounded loop with a buggy condition would hot-spin
  # workers and the database. 0 means the engine default (WIGGLE_LOOP_MAX_ITERATIONS, 10,000).
  DoWhile = Struct.new(:condition, :body, :max_iterations) do
    include Node

    def initialize(condition, body, max_iterations: 0) = super(condition, body, max_iterations)
  end

  # A compiled workflow: the definition sent to the server (topology only). Handlers are bound
  # separately on a worker, by name.
  FlowSpec = Struct.new(:name, :version, :definition, :queues, keyword_init: true)

  # A workflow's topology as declarative data. #compile validates the shape and turns it into a
  # FlowSpec you register and start.
  #
  # +version+ pins an explicit version instead of the auto content hash. Use it to choose a stable,
  # human-meaningful number (or to match another client) -- but then YOU own bumping it when the graph
  # changes: the server overwrites the stored graph for a reused version, which affects instances
  # already running on it. Leave it unset for the safe content-addressed default.
  class Graph
    attr_reader :name, :steps, :version, :default_queue

    def initialize(name, steps = [], version: nil, default_queue: nil)
      @name = name
      @steps = steps
      @version = version
      @default_queue = default_queue
    end

    def compile
      raise ArgumentError, "workflow name is required" if name.nil? || name.to_s.strip.empty?
      raise ArgumentError, "workflow has no steps" if steps.nil? || steps.empty?

      graph = GraphStore.new(name, default_queue || name)
      root = Builder.new(graph, nil, root: true)
      root.append_nodes(steps)
      end_id = graph.add_end
      root.wire_open_to(end_id)
      graph.start_node ||= end_id

      queues = graph.queues.to_a.sort
      definition = {
        "name" => name,
        "startNode" => graph.start_node,
        "nodes" => graph.nodes.values,
        "queues" => queues,
        "executionMode" => "SERVER"
      }
      v = version.nil? ? Versioning.content_version(definition) : Versioning.check_version(version)
      FlowSpec.new(name: name, version: v, definition: { "version" => v }.merge(definition), queues: queues)
    end
  end

  # The accumulating node store shared by a workflow and all its nested branches.
  # @api private
  class GraphStore
    attr_reader :name, :default_queue, :nodes, :queues
    attr_accessor :start_node

    def initialize(name, default_queue)
      @name = name
      @default_queue = default_queue
      @nodes = {}
      @queues = Set.new
      @reserved = Set.new
      @start_node = nil
      @counter = 0
    end

    def add_worker(kind, name, queue, retry_policy)
      reserve(name)
      nid = next_id
      q = queue || default_queue
      @queues << q
      @nodes[nid] = { "id" => nid, "kind" => kind, "name" => name, "activity" => "#{@name}##{name}",
                      "queue" => q, "retry" => (retry_policy || Retry.forever).to_json_hash }
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

    def add_subworkflow(name, child_workflow)
      reserve(name)
      nid = next_id("sub")
      # the child workflow's name travels in `activity`; the engine starts it (no worker handler)
      @nodes[nid] = { "id" => nid, "kind" => "SUB_WORKFLOW", "name" => name, "activity" => child_workflow }
      nid
    end

    def add_dynfork(name, items_key, item_key)
      reserve(name)
      nid = next_id("dynfork")
      @nodes[nid] = { "id" => nid, "kind" => "DYN_FORK", "name" => name, "itemsKey" => items_key, "itemKey" => item_key }
      nid
    end

    # The mandatory merge node after a fork's join: a TASK bound by name like any step, carrying the
    # fork's armNames so the engine can stage each isolated branch's result for the handler, and
    # strip it afterward.
    def add_combine(name, arms)
      nid = add_worker("TASK", name, nil, nil)
      @nodes[nid]["armNames"] = arms.dup
      nid
    end

    # The mandatory merge node after a forEach's join: a TASK bound by name whose collectKey is the
    # scratch key the engine stages the collected item results under.
    def add_for_each_combine(name, for_each_name)
      nid = add_worker("TASK", name, nil, nil)
      @nodes[nid]["collectKey"] = FOR_EACH_SCRATCH_PREFIX + for_each_name
      nid
    end

    def add_join(expected)
      nid = next_id("join")
      node = { "id" => nid, "kind" => "JOIN", "name" => nid }
      node["expected"] = expected if expected.positive? # dynamic joins carry their width in the runtime group
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

    def set_branches(fork_id, starts)
      @nodes[fork_id]["branches"] = starts.dup
    end

    private

    def next_id(prefix = "n")
      @counter += 1
      "#{prefix}#{@counter}"
    end

    def reserve(name)
      raise ArgumentError, "a step is missing a name" if name.nil? || name.to_s.empty?
      raise ArgumentError, "duplicate step name '#{name}'" if @reserved.include?(name)

      @reserved << name
    end
  end

  # Open-ends wiring over the declarative node list. One per node stream (the root, plus one per
  # nested branch / case / body); they all share a single GraphStore.
  # @api private
  class Builder
    attr_reader :start, :open

    def initialize(graph, enclosing_join, root: false)
      @g = graph
      @enclosing_join = enclosing_join
      @root = root
      @open = [] # [node_id, :next|:alt] ends waiting to be wired
      @start = nil
    end

    def append_nodes(nodes)
      nodes.each { |n| append_node(n) }
    end

    def wire_open_to(target)
      @open.each { |nid, edge| @g.wire(nid, edge, target) }
      @open = []
    end

    private

    def sub(enclosing_join) = Builder.new(@g, enclosing_join)

    # Wire the current open ends to +node_id+; the very first node becomes this stream's start.
    def attach(node_id)
      if @open.any?
        wire_open_to(node_id)
      elsif @start.nil?
        @start = node_id
        @g.start_node = node_id if @root
      end
    end

    def chain(node_id)
      attach(node_id)
      @open = [[node_id, :next]]
    end

    def append_node(n)
      case n
      when Step, Effect
        nid = @g.add_worker("TASK", n.name, n.queue, n.retry)
        @g.nodes[nid]["compensable"] = true if n.compensate
        chain(nid)
      when Gate
        nid = @g.add_worker("PREDICATE", n.name, n.queue, n.retry)
        attach(nid)
        @g.wire(nid, :alt, @enclosing_join || @g.add_end("gated:#{n.name}"))
        @open = [[nid, :next]]
      when Sleep
        chain(@g.add_timer("SLEEP", n.name, (n.seconds * 1000).to_i + n.millis.to_i, reserve: false))
      when AwaitSignal then append_await_signal(n)
      when SubWorkflow then chain(@g.add_subworkflow(n.name, child_name(n.workflow)))
      when Fork then append_fork(n)
      when ForEach then append_for_each(n)
      when Choose then append_choose(n)
      when DoWhile then append_do_while(n)
      else raise TypeError, "not a workflow node: #{n.inspect}"
      end
    end

    def append_await_signal(n)
      nid = @g.add_timer("SIGNAL", n.name, (n.timeout * 1000).to_i, reserve: true)
      attach(nid)
      if n.escalation.nil?
        @open = [[nid, :next]] # delivery path only
        return
      end
      raise ArgumentError, "await_signal escalation needs a positive timeout" unless n.timeout.positive?

      # delivery path is `next`; the escalation branch hangs off `alt` and its tail rejoins, so both
      # continue to whatever follows -- the SIGNAL next/altNext shape the Java client emits.
      s = sub(@enclosing_join)
      s.append_nodes(n.escalation)
      raise ArgumentError, "escalation branch of '#{n.name}' defines no steps" if s.start.nil?

      @g.wire(nid, :alt, s.start)
      @open = [[nid, :next], *s.open]
    end

    def append_fork(n)
      raise ArgumentError, "fork needs at least two branches" if n.branches.nil? || n.branches.size < 2
      if n.combine.nil? || n.combine.to_s.empty?
        raise ArgumentError, "fork needs a combine step name (Fork.new(..., combine: ...)): branches rejoin " \
                             "at an explicit merge handler; there is no implicit fold"
      end

      fork_id = @g.add_fork
      attach(fork_id)
      join_id = @g.add_join(n.branches.size)
      starts = n.branches.map { |b| build_branch(b, join_id) }
      @g.set_branches(fork_id, starts)
      combine_id = @g.add_combine(n.combine, n.branches.map(&:name))
      @g.wire(join_id, :next, combine_id)
      @open = [[combine_id, :next]]
    end

    def append_for_each(n)
      if n.combine.nil? || n.combine.to_s.empty?
        raise ArgumentError, "for_each needs a combine step name (ForEach.new(..., combine: ...)): item " \
                             "results rejoin at an explicit merge handler; there is no implicit fold"
      end

      fork_id = @g.add_dynfork(n.name, n.over, n.over)
      attach(fork_id)
      join_id = @g.add_join(0) # 0 = dynamic width
      template_start = build_branch(Branch.new(n.name, n.body), join_id)
      @g.set_branches(fork_id, [template_start])
      @g.wire(fork_id, :next, join_id) # empty-collection skip (past the combine, engine-side)
      combine_id = @g.add_for_each_combine(n.combine, n.name)
      @g.wire(join_id, :next, combine_id)
      @open = [[combine_id, :next]]
    end

    def append_do_while(n)
      s = sub(@enclosing_join)
      s.append_nodes(n.body)
      raise ArgumentError, "do_while body defines no steps" if s.start.nil?
      if n.max_iterations.negative?
        raise ArgumentError, "do_while '#{n.condition}': max_iterations must be positive (0 = engine default)"
      end

      cond_id = @g.add_worker("PREDICATE", n.condition, nil, nil)
      # -1 = the engine-default budget sentinel, mirroring the Java client's two-arg doWhile.
      @g.nodes[cond_id]["loopBudget"] = n.max_iterations.positive? ? n.max_iterations : -1
      attach(s.start)                   # enter at the body
      s.wire_open_to(cond_id)           # body tail -> condition
      @g.wire(cond_id, :next, s.start)  # true: loop back to the body
      @open = [[cond_id, :alt]]         # false: continue onward
    end

    def append_choose(n)
      cases = n.cases
      has_default = validate_choose(cases)
      n_guards = cases.size - (has_default ? 1 : 0)

      guard_ids = cases.first(n_guards).map { |c| @g.add_worker("PREDICATE", c.when_, nil, nil) }
      attach(guard_ids.first)
      @open = []
      guard_ids.each_cons(2) { |a, b| @g.wire(a, :alt, b) } # each guard's false path -> next guard
      guard_ids.each_with_index { |gid, i| collect_case(cases[i], gid, :next) } # true path -> its branch

      if has_default
        collect_case(cases.last, guard_ids.last, :alt)
      else
        @open << [guard_ids.last, :alt] # no match skips the choose entirely
      end
    end

    def build_branch(branch, join_id)
      s = sub(join_id)
      s.append_nodes(branch.steps)
      raise ArgumentError, "branch '#{branch.name}' defines no steps" if s.start.nil?

      s.wire_open_to(join_id)
      s.start
    end

    def collect_case(kase, guard_id, edge)
      s = sub(@enclosing_join)
      s.append_nodes(kase.then_)
      raise ArgumentError, "a choose case defines no steps" if s.start.nil?

      @g.wire(guard_id, edge, s.start)
      @open.concat(s.open)
    end

    def validate_choose(cases)
      raise ArgumentError, "choose needs at least one case" if cases.nil? || cases.empty?
      raise ArgumentError, "the otherwise (default) case must be last" if cases[0...-1].any? { |c| c.when_.nil? }

      has_default = cases.last.when_.nil?
      raise ArgumentError, "choose needs at least one guarded case" if has_default && cases.size == 1

      has_default
    end

    def child_name(workflow)
      child = case workflow
              when String then workflow
              when FlowSpec, Graph then workflow.name
              else raise TypeError, "workflow must be a Graph, FlowSpec, or workflow name"
              end
      raise ArgumentError, "child workflow name is required" if child.nil? || child.empty?

      child
    end
  end

  # Content-addressed workflow versions.
  module Versioning
    MAX_VERSION = 0x7FFFFFFF

    module_function

    def check_version(version)
      raise TypeError, "version must be an Integer" unless version.is_a?(Integer)
      raise ArgumentError, "version must be in 1..#{MAX_VERSION}" unless (1..MAX_VERSION).cover?(version)

      version
    end

    # A deterministic, positive 31-bit content hash of the graph's STRUCTURE (name, node contents,
    # edge topology, execution mode) -- independent of the incidental node-id numbering. The same
    # structure always hashes the same, so re-registering is idempotent and the server de-duplicates it.
    #
    # Node ids are relabelled by a deterministic breadth-first walk from the start node (edges visited
    # as next, altNext, then branches in order), so changing how ids are minted never changes the hash.
    # The canonical encoding matches the Python client's byte for byte, so the same topology declared
    # in Ruby or Python registers as the same version.
    def content_version(definition)
      nodes = definition["nodes"].to_h { |n| [n["id"], n] }
      canon = {}
      queue = [definition["startNode"]]
      until queue.empty?
        nid = queue.shift
        next if canon.key?(nid) || !nodes.key?(nid)

        canon[nid] = "c#{canon.size}"
        edges(nodes[nid]).each { |t| queue << t if !t.nil? && nodes.key?(t) && !canon.key?(t) }
      end
      nodes.keys.sort.each { |nid| canon[nid] ||= "c#{canon.size}" } # any unreachable nodes, deterministically

      canon_nodes = canon.sort_by { |_, label| label[1..].to_i }.map do |nid, label|
        n = nodes[nid].merge("id" => label)
        n["next"] = canon[n["next"]] if n.key?("next")
        n["altNext"] = canon[n["altNext"]] if n.key?("altNext")
        n["branches"] = n["branches"].map { |b| canon[b] } if n.key?("branches")
        n
      end

      material = {
        "name" => definition["name"],
        "startNode" => canon[definition["startNode"]],
        "nodes" => canon_nodes,
        "executionMode" => definition["executionMode"]
      }
      digest = Digest::SHA256.digest(CanonicalJson.dump(material)).bytes
      v = ((digest[0] & 0x7F) << 24) | (digest[1] << 16) | (digest[2] << 8) | digest[3]
      v.zero? ? 1 : v
    end

    def edges(node) = [node["next"], node["altNext"], *node.fetch("branches", [])]
  end

  # Sorted-key, compact, ASCII-only JSON -- the same bytes as Python's
  # +json.dumps(obj, sort_keys=True, separators=(",", ":"))+, so content hashes agree across clients.
  # @api private
  module CanonicalJson
    ESCAPES = { '"' => '\\"', "\\" => "\\\\", "\n" => "\\n", "\r" => "\\r", "\t" => "\\t",
                "\b" => "\\b", "\f" => "\\f" }.freeze

    module_function

    def dump(obj)
      case obj
      when Hash then "{#{obj.sort_by { |k, _| k.to_s }.map { |k, v| "#{string(k.to_s)}:#{dump(v)}" }.join(',')}}"
      when Array then "[#{obj.map { |v| dump(v) }.join(',')}]"
      when String, Symbol then string(obj.to_s)
      when true then "true"
      when false then "false"
      when nil then "null"
      when Integer then obj.to_s
      when Float then float(obj)
      else raise TypeError, "cannot canonicalise #{obj.class}"
      end
    end

    def string(s)
      out = +'"'
      s.each_char do |ch|
        cp = ch.ord
        out << if ESCAPES.key?(ch) then ESCAPES[ch]
               elsif cp < 0x20 || (cp > 0x7F && cp <= 0xFFFF) then format("\\u%04x", cp)
               elsif cp > 0xFFFF
                 c = cp - 0x10000
                 format("\\u%04x\\u%04x", 0xD800 + (c >> 10), 0xDC00 + (c & 0x3FF))
               else ch
               end
      end
      out << '"'
    end

    # Python's float repr: shortest round-trip digits, exponent form outside [1e-4, 1e16).
    def float(f)
      raise ArgumentError, "non-finite number in a workflow definition" unless f.finite?

      s = f.to_s # Ruby also prints the shortest round-trip digits
      return s unless s.include?("e")

      mantissa, exp = s.split("e")
      mantissa = mantissa.sub(/\.0\z/, "")
      sign = exp.start_with?("-") ? "-" : "+"
      "#{mantissa}e#{sign}#{exp.delete('+-').rjust(2, '0')}"
    end
  end
end
