# frozen_string_literal: true

require "etc"
require "set"

module Wiggle
  # The data plane: a worker pulls tasks it has capacity for, runs the matching handler, and reports
  # the result. Workers hold no durable state -- a crash loses at most the in-flight step, which the
  # server re-leases and re-runs.
  #
  # Bind handlers to workflow steps by name, then #start to pull and run work. Topology is registered
  # separately (Client#register of a compiled Graph, or by another client).
  #
  #   worker = Wiggle::Worker.new(client, "worker-1")
  #     .handle("order", "validate") { |o| o.merge("status" => "VALIDATED") }
  #     .handle_gate("order", "in-stock") { |o| o["quantity"].positive? }
  #   worker.start          # background threads; call #stop to drain
  class Worker
    attr_reader :worker_id

    # +concurrency+ caps in-flight handlers (default: CPU count). Times are in seconds.
    # +queues+ overrides the queues discovered from the graph. +await_registration+ rides out a
    # startup race where the graph is registered after this worker starts.
    def initialize(client, worker_id, concurrency: nil, lease: 30, long_poll_wait: 10, idle_backoff: 0.2,
                   error_backoff: 2, queues: nil, await_registration: 0, logger: Wiggle.logger)
      @client = client
      @worker_id = worker_id
      @concurrency = concurrency || Etc.nprocessors || 4
      @lease_ms = (lease * 1000).to_i
      @wait_ms = (long_poll_wait * 1000).to_i
      @idle_backoff = idle_backoff
      @error_backoff = error_backoff
      @explicit_queues = queues.nil? || queues.empty? ? nil : Set.new(queues)
      @await_registration = await_registration
      @log = logger

      @handlers = {}
      @queues = Set.new
      @claims = [] # [workflow, step, expected kind]
      @handler_sets = [] # [workflow, candidates]
      @running = Latch.new
      @inflight = 0
      @lock = Mutex.new
      @pool = []
      @tasks = nil
      @poll_thread = nil
    end

    # Bind a handler to one step of an already-registered workflow, by name -- no topology
    # re-declaration. The handler (a block or any callable) takes the context and returns the new
    # context, which REPLACES the previous context server-side (no diff, no merge; nil leaves it
    # untouched). Which queue the step polls is discovered from the graph on #start, which also fails
    # fast if +step+ does not exist (or is the wrong kind).
    def handle(workflow, step, callable = nil, &blk)
      fn = handler!(callable, blk)
      bind(workflow, step, "TASK", ->(ctx) { fn.call(ctx) })
    end

    # Bind the mandatory merge step that follows a fork's join (or a forEach's). The handler receives
    # the context with each branch's result staged under the branch's name (or every item's final
    # value under the forEach's name), and must return the COMPLETE post-join context: the engine
    # REPLACES the context with it -- keys it omits do not survive the join.
    def handle_combine(workflow, step, callable = nil, &blk)
      fn = handler!(callable, blk)
      bind(workflow, step, "COMBINE", ->(ctx) { fn.call(ctx) })
    end

    # Bind a predicate (gate / choose guard / do-while condition) step by name; the handler's
    # truthiness picks the branch.
    def handle_gate(workflow, step, callable = nil, &blk)
      fn = handler!(callable, blk)
      bind(workflow, step, "PREDICATE", ->(ctx) { fn.call(ctx) ? true : false })
    end

    # Bind a side-effect step by name; the handler's return is ignored and the context is unchanged.
    def handle_effect(workflow, step, callable = nil, &blk)
      fn = handler!(callable, blk)
      bind(workflow, step, "TASK", lambda { |ctx|
        fn.call(ctx)
        nil
      })
    end

    # Bind the undo of a compensable step (one declared +compensate: true+ in the topology). The
    # handler receives a Wiggle::Compensation (the step's input/result snapshots) and runs in the
    # reverse pass after the instance fails. #start refuses a worker that serves a compensable step's
    # forward handler without also binding its compensator -- the undo task is minted on the same
    # queue, and a worker that can do but not undo would strand the reverse pass.
    def handle_compensation(workflow, step, callable = nil, &blk)
      fn = handler!(callable, blk)
      raise ArgumentError, "workflow and step are required" if blank?(workflow) || blank?(step)

      activity = "#{workflow}##{step}#compensate"
      raise ArgumentError, "duplicate compensator for activity '#{activity}'" if @handlers.key?(activity)

      @handlers[activity] = Binder.compensation_wrapper(fn)
      @claims << [workflow, step, "COMPENSATE"]
      self
    end

    # Bind a whole object's methods as step handlers in one call. Each public method that takes a
    # context becomes a handler; on #start the worker matches it to a step of +workflow+ by
    # case-insensitive name (+in_stock+ / +inStock+ / +in_stock?+ all match a step named +in-stock+).
    # The graph is the source of truth for the exact step name and whether it is a gate; side effects
    # are marked with Handlers.effect (or listed in +effects:+ for a plain object).
    #
    # +workflow+ may be passed here or declared on the object (a Wiggle::Handlers subclass's
    # +workflow "name"+, or any object answering #workflow). Two methods whose names collide under
    # case-folding are rejected here (ambiguous); a method matching no step, or a kind clash with the
    # graph, is caught on #start during reconciliation.
    def register_handlers(handlers, workflow: nil, effects: [])
      wf = workflow || (handlers.respond_to?(:workflow) ? handlers.workflow : nil)
      if blank?(wf)
        raise ArgumentError, "register_handlers needs a workflow name: pass workflow: ... or declare " \
                             "`workflow \"name\"` on the handlers class"
      end

      @handler_sets << [wf, Binder.scan(handlers, effects: effects)]
      self
    end

    # Reconcile the bindings against the registered graphs, then start polling on background threads.
    # With a block, runs it and then #stop's (draining) -- returning the block's value.
    def start(&blk)
      return run_started(&blk) if @running.set?

      reconcile if @claims.any? || @handler_sets.any?
      @running.set
      @tasks = Thread::Queue.new
      @pool = Array.new(@concurrency) do |i|
        Thread.new do
          Thread.current.name = "wiggle-#{@worker_id}-#{i}"
          while (task = @tasks.pop)
            run(task)
          end
        end
      end
      @poll_thread = Thread.new do
        Thread.current.name = "wiggle-poll-#{@worker_id}"
        poll_loop
      end
      @log.info("worker #{@worker_id} polling queues #{served_queues.to_a.sort} with concurrency #{@concurrency}")
      run_started(&blk)
    end

    # Stop polling and drain: tasks already claimed still run and report, up to +timeout+ seconds.
    def stop(timeout: 10)
      return unless @running.set?

      @running.clear
      @poll_thread&.join(timeout)
      @tasks&.close
      deadline = monotonic + timeout
      @pool.each { |t| t.join([deadline - monotonic, 0].max) }
      nil
    end

    def running? = @running.set?

    def in_flight = @lock.synchronize { @inflight }

    # Start and block until interrupted (Ctrl-C / SIGTERM), then drain.
    def run_forever
      start
      %w[INT TERM].each { |sig| trap(sig) { Thread.new { stop } } }
      @running.wait_cleared
      @pool.each(&:join)
    end

    # The queues this worker polls: the explicit +queues:+ if given, else those learned from the graph.
    def served_queues = @explicit_queues || @queues

    # @api private (visible for tests)
    attr_reader :handlers

    # Check every #handle-bound claim against the server's registered graph, and learn the queue each
    # claimed step polls (a name-only binding has no other way to know it). Fails fast on a typo'd
    # step name or a kind mismatch, so a bad binding is caught at start rather than as a silent
    # runtime "no handler" much later.
    # @api private (visible for tests)
    def reconcile
      @claims.map(&:first).uniq.sort.each { |wf| reconcile_claims(wf) }
      @handler_sets.each { |wf, candidates| match_handler_set(wf, candidates) }
    end

    private

    def run_started
      return self unless block_given?

      begin
        yield self
      ensure
        stop
      end
    end

    def reconcile_claims(wf)
      graph = fetch_graph(wf)
      nodes = {}
      graph.fetch("nodes", []).each do |n|
        nodes[n["activity"]] = n if %w[TASK PREDICATE].include?(n["kind"]) && n.key?("activity")
      end
      served = Set.new
      claims = @claims.select { |w, _, _| w == wf }
      claims.each do |_, step, kind|
        activity = "#{wf}##{step}"
        node = nodes[activity]
        if node.nil?
          avail = nodes.keys.map { |a| a.split("#", 2)[1] }.sort
          raise ArgumentError, "no step '#{step}' in registered workflow '#{wf}' (available steps: #{avail.inspect})"
        end
        is_combine = Binder.combine?(node)
        case kind
        when "COMPENSATE"
          if node["kind"] != "TASK" || is_combine || !node["compensable"]
            raise ArgumentError, "step '#{step}' of workflow '#{wf}' is not declared compensate: true in the " \
                                 "topology; handle_compensation binds only compensable steps"
          end
        when "COMBINE"
          raise ArgumentError, "activity '#{activity}' is not a fork combine; bind it with handle()" unless is_combine
        else
          if is_combine
            raise ArgumentError, "activity '#{activity}' is a fork combine; bind it with handle_combine() -- its " \
                                 "return is the complete post-join context (there is no implicit fold)"
          end
          if node["kind"] != kind
            verb = node["kind"] == "PREDICATE" ? "handle_gate" : "handle"
            raise ArgumentError, "activity '#{activity}' is a #{node['kind']} in the graph but was bound as " \
                                 "#{kind}; use #{verb}() instead"
          end
        end
        @queues << node.fetch("queue", wf)
        served << activity
      end
      # Pairing: a forward handler on a compensable step requires its compensator here too -- the undo
      # task lands on the same queue this worker polls, so "can do but not undo" would strand the
      # reverse pass at claim time.
      compensated = claims.select { |_, _, k| k == "COMPENSATE" }.map { |_, s, _| s }
      claims.each do |_, step, kind|
        next if kind != "TASK" || compensated.include?(step)

        node = nodes["#{wf}##{step}"]
        next unless node && node["compensable"]

        raise ArgumentError, "step '#{step}' of workflow '#{wf}' is declared compensate: true but this worker " \
                             "binds no compensator; add handle_compensation('#{wf}', '#{step}') { |c| ... }"
      end
      unclaimed = (nodes.keys - served.to_a).map { |a| a.split("#", 2)[1] }.sort
      # info, not an error: a polyglot worker may intentionally serve a subset
      @log.info("workflow '#{wf}' has steps served by no handler on this worker: #{unclaimed}") if unclaimed.any?
    end

    # Match a #register_handlers object's methods to +wf+'s steps (fetched here -- the binder itself is
    # pure) and install the resulting bindings.
    def match_handler_set(wf, candidates)
      result = Binder.bind(wf, candidates, fetch_graph(wf))
      result.bindings.each do |b|
        raise ArgumentError, "duplicate handler for activity '#{b.activity}'" if @handlers.key?(b.activity)

        @handlers[b.activity] = b.handler
        @queues << b.queue
      end
      return if result.unserved.empty?

      @log.info("workflow '#{wf}' has steps served by no handler on this worker: #{result.unserved}")
    end

    # Fetch the registered graph, optionally waiting out a registration race (the authoring client may
    # still be starting up). Beyond the grace window, a missing graph is fatal.
    def fetch_graph(workflow)
      deadline = monotonic + @await_registration
      begin
        @client.get_workflow(workflow)
      rescue GRPC::NotFound
        if monotonic < deadline
          Kernel.sleep(0.25)
          retry
        end
        raise ArgumentError, "workflow '#{workflow}' is not registered; register its graph before starting a " \
                             "worker that binds handlers to it (or pass await_registration:)"
      end
    end

    def bind(workflow, step, kind, wrapper)
      raise ArgumentError, "workflow and step are required" if blank?(workflow) || blank?(step)

      activity = "#{workflow}##{step}"
      raise ArgumentError, "duplicate handler for activity '#{activity}'" if @handlers.key?(activity)

      @handlers[activity] = wrapper
      @claims << [workflow, step, kind]
      self
    end

    def handler!(callable, blk)
      fn = callable || blk
      raise ArgumentError, "pass a handler block or a callable" unless fn
      raise ArgumentError, "pass a handler block or a callable, not both" if callable && blk
      raise ArgumentError, "handler must respond to #call" unless fn.respond_to?(:call)

      fn
    end

    def poll_loop
      while @running.set?
        begin
          poll_once
        rescue StandardError => e # a poll failure must not kill the loop
          break unless @running.set?

          @log.warn("poll failed: #{e.class}: #{e.message}")
          @running.wait_cleared(@error_backoff)
        end
      end
    end

    def poll_once
      free = @lock.synchronize { @concurrency - @inflight }
      unless free.positive?
        @running.wait_cleared(@idle_backoff)
        return
      end
      result = @client.poll(@worker_id, served_queues, free, @lease_ms, @wait_ms)
      if result.tasks.empty?
        # honour the server's backpressure hold-off if it is shedding load
        backoff = result.retry_after_millis.positive? ? result.retry_after_millis / 1000.0 : @idle_backoff
        @running.wait_cleared(backoff)
        return
      end
      result.tasks.each do |task|
        @lock.synchronize { @inflight += 1 }
        @tasks << task
      end
    end

    def run(task)
      execute(task)
    rescue StandardError => e
      @log.error("task #{task.task_id} (#{task.activity}) could not be settled: #{e.class}: #{e.message}")
    ensure
      @lock.synchronize { @inflight -= 1 }
    end

    def execute(task)
      handler = @handlers[task.activity]
      if handler.nil?
        @client.fail(task.task_id, task.lease_owner, "no handler registered for activity '#{task.activity}'",
                     retryable: false)
        return
      end

      begin
        ctx, combine_base = Binder.unstage(Convert.from_value(task.context), task.arm_names.to_a,
                                           task.collect_key, task.step_name)
      rescue ArgumentError => e
        @client.fail(task.task_id, task.lease_owner, e.message, retryable: false)
        return
      end
      heartbeat = start_heartbeat(task)
      # A forEach item step's parameter is the ITEM's value (scalars included); the frozen base and
      # the element's index/source key ride on Wiggle::Current. A combine's base rides there too.
      item = !task.base_context.nil?
      scope = Current::Scope.new(
        attempt: task.attempt, step_name: task.step_name, instance_id: task.instance_id,
        base: item ? Convert.from_value(task.base_context) : combine_base,
        has_base: item || !combine_base.nil?, item: item,
        index: task.item_index, map_key: task.item_map_key.empty? ? nil : task.item_map_key
      )
      started_at = (Time.now.to_f * 1000).to_i
      t0 = monotonic
      begin
        result = Current.with_scope(scope) { handler.call(ctx) }
        heartbeat.set
        report = StepReport.new(node_id: task.node_id, started_at: started_at,
                                finished_at: started_at + ((monotonic - t0) * 1000).to_i, events: scope.emitted.dup)
        if task.kind == "PREDICATE"
          report.predicate = result ? true : false
        else
          report.merge = result
        end
        begin
          @client.report_steps(task.task_id, task.lease_owner, [report])
        rescue ReportRefused => e # the lease was lost; the task re-runs elsewhere
          @log.warn(e.message)
        end
      rescue PermanentError => e
        @client.fail(task.task_id, task.lease_owner, e.message, retryable: false)
      rescue StandardError, ScriptError => e # a handler failure is a step failure, retried
        @log.debug { "step #{task.step_name} of #{task.instance_id} failed: #{e.class}: #{e.message}" }
        @client.fail(task.task_id, task.lease_owner, "#{e.class}: #{e.message}", retryable: true)
      ensure
        heartbeat.set
      end
    end

    # Extend the lease periodically while a handler runs, so a slow step keeps its claim.
    def start_heartbeat(task)
      stop = Latch.new
      interval = [@lease_ms / 2000.0, 1.0].max # half the lease, in seconds
      Thread.new do
        Thread.current.name = "wiggle-hb-#{task.task_id}"
        until stop.wait_set(interval)
          begin
            @client.heartbeat(task.task_id, task.lease_owner, @lease_ms)
          rescue StandardError
            break # the step will just fail/retry if the lease is lost
          end
        end
      end
      stop
    end

    def blank?(s) = s.nil? || s.to_s.strip.empty?

    def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    # A boolean flag threads can wait on, in either direction, with a timeout (like Python's
    # threading.Event, plus waiting for it to CLEAR -- which is how backoffs end early on #stop).
    # @api private
    class Latch
      def initialize
        @mutex = Mutex.new
        @cond = ConditionVariable.new
        @set = false
      end

      def set? = @mutex.synchronize { @set }

      def set = change(true)

      def clear = change(false)

      # Wait until set (or +timeout+ seconds pass); returns whether it is set.
      def wait_set(timeout = nil) = wait_for(true, timeout)

      # Wait until cleared (or +timeout+ seconds pass); returns whether it is cleared.
      def wait_cleared(timeout = nil) = !wait_for(false, timeout)

      private

      def change(value)
        @mutex.synchronize do
          @set = value
          @cond.broadcast
        end
        self
      end

      def wait_for(value, timeout)
        @mutex.synchronize do
          deadline = timeout && (Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout)
          while @set != value
            if deadline
              remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
              break unless remaining.positive?

              @cond.wait(@mutex, remaining)
            else
              @cond.wait(@mutex)
            end
          end
          @set
        end
      end
    end
  end
end
