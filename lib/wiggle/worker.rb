# frozen_string_literal: true

require "etc"
require "set"

module Wiggle
  # Worker tuning, immutable; build on WorkerOptions.defaults with the +with_*+ methods. Durations
  # are seconds. Defaults are chosen for interactive workloads, not throughput benchmarks.
  WorkerOptions = Struct.new(:concurrency, :lease, :long_poll_wait, :idle_backoff, :error_backoff, :queues,
                             :await_registration) do
    def self.defaults = new(Etc.nprocessors || 4, 30, 10, 0.2, 2, [].freeze, 0).freeze

    def with_concurrency(c) = with(concurrency: c)

    def with_lease(seconds) = with(lease: seconds)

    def with_long_poll_wait(seconds) = with(long_poll_wait: seconds)

    # Poll only these queues instead of the ones discovered from the registered graphs.
    def with_queues(*only) = with(queues: only.flatten.map(&:to_s).freeze)

    # On start, wait up to this long for a handler's workflow to be registered (a startup race with
    # the authoring service) instead of failing at once.
    def with_await_registration(seconds) = with(await_registration: seconds)

    private

    def with(**changes)
      dup.tap { |o| changes.each { |k, v| o[k] = v } }.freeze
    end
  end

  # The data plane: pulls tasks it has capacity for, runs the matching handler, and reports the
  # result. Workers hold no durable state -- a crash loses at most the in-flight step, which the
  # server re-leases and re-runs.
  #
  #   worker = Wiggle::Worker.new(client, "worker-1")
  #   worker.register_handler(OrderHandlers.new)   # its workflow from `for_flow`
  #   worker.start
  #   ...
  #   worker.close
  class Worker
    attr_reader :worker_id, :options

    def initialize(client, worker_id, options = WorkerOptions.defaults, logger: Wiggle.logger)
      @client = client
      @worker_id = worker_id
      @options = options
      @log = logger
      @registrations = [] # [workflow, handler object, version or nil]
      @handlers = {}      # activity (or "activity@version") -> wrapper
      @queues = Set.new
      @running = Latch.new
      @inflight = 0
      @lock = Mutex.new
      @pool = []
    end

    # Binds a handler object's methods as this worker's step implementations, matched to the
    # registered graph by name on #start (case/style-insensitive, so +in_stock?+ serves +in-stock+).
    #
    #   register_handler(handlers)                  # workflow from the class's `for_flow`
    #   register_handler("order-fulfilment", handlers)
    #   register_handler(handlers, 3)               # this version only: checked against that exact
    #                                               # graph, and only its tasks are claimed
    #
    # Without a version a worker serves every version of the workflow, which is usually right: step
    # names are stable across versions. If ANY registration is unversioned, the worker claims every
    # version.
    def register_handler(*args)
      workflow, handler, version =
        case args
        in [String => wf, h] then [wf, h, nil]
        in [h, Integer => v] then [nil, h, v]
        in [h] then [nil, h, nil]
        else raise ArgumentError, "register_handler takes (handler), (workflow, handler) or (handler, version)"
        end
      workflow ||= ForFlow.flow_name(handler.class)
      if workflow.nil? || workflow.empty?
        raise ArgumentError, "#{handler.class} names no workflow: declare `for_flow \"name\"` on it (extend " \
                             "Wiggle::ForFlow), or pass the name to register_handler"
      end
      raise ArgumentError, "worker #{@worker_id} is already running; register handlers before start" if running?

      @registrations << [workflow, handler, version]
      self
    end

    def start
      return self if running?

      reconcile unless @registrations.empty?
      @running.set
      @tasks = Thread::Queue.new
      @pool = Array.new(@options.concurrency) do |i|
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
      @log.info("worker #{@worker_id} polling queues #{served_queues.to_a.sort} with concurrency #{@options.concurrency}")
      self
    end

    # Stops polling and drains: tasks already claimed still run and report.
    def close
      return unless running?

      @running.clear
      @poll_thread&.join(@options.long_poll_wait + 5)
      @tasks&.close
      @pool.each(&:join)
      nil
    end

    def running? = @running.set?

    def in_flight = @lock.synchronize { @inflight }

    # @api private (visible for tests)
    attr_reader :handlers

    # @api private (visible for tests)
    def served_queues = @options.queues.empty? ? @queues : Set.new(@options.queues)

    # Matches every registration against its registered graph and installs the bindings, learning the
    # queue each served step polls. Fails fast on a method matching no step or a shape that clashes
    # with the graph.
    # @api private (visible for tests)
    def reconcile
      @registrations.each do |workflow, handler, version|
        set = HandlerBinder.scan(workflow, handler)
        result = HandlerBinder.bind(set, fetch_graph(workflow, version))
        result.bindings.each do |b|
          key = version ? "#{b.activity}@#{version}" : b.activity
          raise ArgumentError, "duplicate handler for activity '#{b.activity}'" if @handlers.key?(key)

          @handlers[key] = b.handler
          @queues << b.queue
        end
        next if result.unserved.empty?

        # info, not an error: a polyglot worker may intentionally serve a subset
        @log.info("workflow '#{workflow}' has steps served by no handler on this worker: #{result.unserved}")
      end
    end

    private

    # [workflow, version] pairs to claim -- empty (every version) unless every registration is versioned.
    def versions
      return [] if @registrations.any? { |_, _, v| v.nil? }

      @registrations.map { |w, _, v| [w, v] }.uniq
    end

    # Fetches the registered graph, waiting out a registration race up to +await_registration+.
    def fetch_graph(workflow, version)
      deadline = monotonic + @options.await_registration
      begin
        @client.get_workflow(workflow, version)
      rescue WiggleApiException => e
        raise unless e.status == 404

        if monotonic < deadline
          Kernel.sleep(0.25)
          retry
        end
        what = version ? "version #{version} of workflow '#{workflow}'" : "workflow '#{workflow}'"
        raise ArgumentError, "#{what} is not registered; register its FlowSpec before starting a worker that " \
                             "serves it (or set WorkerOptions#with_await_registration)"
      end
    end

    def poll_loop
      while running?
        begin
          poll_once
        rescue StandardError => e # a poll failure must not kill the loop
          break unless running?

          @log.warn("poll failed: #{e.class}: #{e.message}")
          @running.wait_cleared(@options.error_backoff)
        end
      end
    end

    def poll_once
      free = @lock.synchronize { @options.concurrency - @inflight }
      unless free.positive?
        @running.wait_cleared(@options.idle_backoff)
        return
      end
      result = @client.poll(@worker_id, served_queues, free, millis(@options.lease), millis(@options.long_poll_wait),
                            versions)
      if result.tasks.empty?
        # honour the server's backpressure hold-off if it is shedding load
        backoff = result.retry_after_millis.positive? ? result.retry_after_millis / 1000.0 : @options.idle_backoff
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
      handler = @handlers["#{task.activity}@#{task.version}"] || @handlers[task.activity]
      if handler.nil?
        @client.fail(task.task_id, task.lease_owner, "no handler registered for activity '#{task.activity}'", false)
        return
      end
      ctx = Convert.from_value(task.context)
      inputs, combine_base = HandlerBinder.unstage(ctx, task.arm_names.to_a, task.collect_key)
      heartbeat = start_heartbeat(task)
      # A forEach item step's parameter is the ITEM's value (scalars included); the frozen base and the
      # element's index/source key ride on Wiggle::Step. A combine's base rides there too.
      item = !task.base_context.nil?
      info = Step::Info.new(task.attempt, task.step_name, task.instance_id,
                            item ? Convert.from_value(task.base_context) : combine_base, item, task.item_index,
                            task.item_map_key.empty? ? nil : task.item_map_key, [])
      started_at = (Time.now.to_f * 1000).to_i
      t0 = monotonic
      begin
        result = Step.with(info) { handler.call(inputs || ctx, combine_base) }
        heartbeat.set
        report = StepReport.new(task.node_id, nil, nil, started_at, started_at + ((monotonic - t0) * 1000).to_i,
                                info.emitted.dup)
        if task.kind == "PREDICATE"
          report.predicate_value = result ? true : false
        else
          report.merge = result
        end
        outcome = @client.report_steps([RunSubmission.new(task.task_id, task.lease_owner, [report], true)])[task.task_id]
        unless outcome&.ok? # the lease was lost; the task re-runs elsewhere
          @log.warn("report of task #{task.task_id} refused (#{outcome&.error_status}): #{outcome&.error}")
        end
      rescue PermanentActivityException => e
        @client.fail(task.task_id, task.lease_owner, e.message, false)
      rescue StandardError, ScriptError => e # a handler failure is a step failure, retried
        @log.debug { "step #{task.step_name} of #{task.instance_id} failed: #{e.class}: #{e.message}" }
        @client.fail(task.task_id, task.lease_owner, "#{e.class}: #{e.message}", true)
      ensure
        heartbeat.set
      end
    end

    # Extends the lease periodically while a handler runs, so a slow step keeps its claim.
    def start_heartbeat(task)
      stop = Latch.new
      interval = [@options.lease / 2.0, 1.0].max
      Thread.new do
        Thread.current.name = "wiggle-hb-#{task.task_id}"
        until stop.wait_set(interval)
          begin
            @client.heartbeat(task.task_id, task.lease_owner, millis(@options.lease))
          rescue StandardError
            break # the step will just fail/retry if the lease is lost
          end
        end
      end
      stop
    end

    def millis(seconds) = (seconds * 1000).round

    def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    # A boolean flag threads can wait on, in either direction, with a timeout -- waiting for it to
    # CLEAR is how backoffs end early on #close.
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
