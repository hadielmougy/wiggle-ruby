# frozen_string_literal: true

require "grpc"
require_relative "proto/wiggle_services_pb"

module Wiggle
  PB = ::Com::Wiggle::Proto

  # COMPENSATING is NOT terminal: the reverse pass is still running its compensators; it settles as
  # COMPENSATED or COMPENSATION_FAILED.
  TERMINAL = %w[COMPLETED FAILED CANCELLED COMPENSATED COMPENSATION_FAILED].freeze

  InstanceView = Struct.new(:id, :workflow, :version, :status, :termination_reason, :error, :context,
                            :created_at, :updated_at, keyword_init: true) do
    def done? = TERMINAL.include?(status)
  end

  ScheduleView = Struct.new(:id, :workflow, :every_millis, :cron, :next_fire_at, :created_at, keyword_init: true)

  # One entry of the event log: a lifecycle transition (+wf.*+) or a handler-emitted event.
  EventView = Struct.new(:seq, :shard, :instance_id, :workflow, :version, :correlation_id, :type,
                         :created_at, :payload, :node_id, :cursor, keyword_init: true)
  EventBatch = Struct.new(:events, :retry_after_millis, keyword_init: true)

  SearchHit = Struct.new(:instance_id, :workflow, :version, :status, :correlation_id, :created_at,
                         :updated_at, :score, :purged, keyword_init: true)
  SearchResult = Struct.new(:hits, :partial, :model, keyword_init: true)

  NodeStats = Struct.new(:node_id, :name, :count, :mean_millis, :p50_millis, :p95_millis, :max_millis,
                         :wait_p50_millis, :wait_p95_millis, keyword_init: true)
  StepStats = Struct.new(:workflow, :version, :nodes, keyword_init: true)

  BacklogSlice = Struct.new(:workflow, :version, :queue, :ready_count, :oldest_available_at, :covered,
                            :coverage, :claimable, :criteria, keyword_init: true)
  BacklogCoverage = Struct.new(:slices, :live_pollers, keyword_init: true)

  # One finished step, as a worker reports it: exactly one of +merge+ (a task's complete next context;
  # nil leaves it unchanged) or +predicate+ (a predicate's branch), with when it ran and the events it
  # emitted as [type, payload] pairs.
  StepReport = Struct.new(:node_id, :merge, :predicate, :started_at, :finished_at, :events, keyword_init: true) do
    def initialize(node_id:, merge: nil, predicate: nil, started_at: 0, finished_at: 0, events: [])
      super
    end
  end

  # What a reported run applied to. Anything but RUNNING means stop here.
  RunApplied = Struct.new(:instance_status, :lease_expires_at, :next_task_id, keyword_init: true)

  # A thin, Ruby-flavoured wrapper over the gRPC control plane: register workflows, start and track
  # instances, deliver signals, manage schedules, read the event feed. Also carries the low-level
  # worker RPCs (poll / report_steps / fail / heartbeat) used by Worker.
  #
  #   Wiggle::Client.open("localhost:8080") do |client|
  #     client.register(wf)
  #     id = client.start(wf, { "orderId" => "A-1" })
  #     client.await_completion(id).status   # => "COMPLETED"
  #   end
  class Client
    # Failures expected in normal operation -- reconcile waiting for a workflow to be registered, a
    # server not yet up -- log at DEBUG; anything else is a genuine error and logs at WARN.
    EXPECTED_FAILURES = [GRPC::NotFound, GRPC::Unavailable].freeze

    # Open a client, yield it, and close it afterwards (returns the block's value).
    def self.open(target = "localhost:8080", **opts)
      client = new(target, **opts)
      return client unless block_given?

      begin
        yield client
      ensure
        client.close
      end
    end

    # +target+ is +host:port+ (an +http://+ / +https://+ / +dns:///+ scheme prefix is stripped).
    #
    # TLS: pass +credentials:+ (a GRPC::Core::ChannelCredentials, e.g. built from your CA and an mTLS
    # key/cert) or +tls: true+ to use the system trust roots. Without either the channel is plaintext.
    #
    # +api_key+ (default: ENV["WIGGLE_API_KEY"]) is presented on every call as
    # +authorization: Bearer <key>+, like the Java client. A key travels in the clear on a plaintext
    # channel, so use one with TLS.
    #
    # +wait_for_ready+ (default true) makes the worker-critical RPCs -- register, get_workflow, poll,
    # report_steps, fail, heartbeat -- wait for the server to become reachable instead of failing fast
    # with UNAVAILABLE. This lets a worker ride out a cold start, a rolling restart, or a reschedule
    # without crashing. Query calls (instance, list, ...) stay fail-fast.
    def initialize(target = "localhost:8080", credentials: nil, tls: false, api_key: ENV.fetch("WIGGLE_API_KEY", nil),
                   wait_for_ready: true, channel_args: {}, logger: Wiggle.logger)
      creds = credentials || (tls ? GRPC::Core::ChannelCredentials.new : :this_channel_is_insecure)
      @stub = PB::WiggleControlPlane::Stub.new(strip_scheme(target), creds, channel_args: channel_args)
      @metadata = api_key.nil? || api_key.strip.empty? ? {} : { "authorization" => "Bearer #{api_key.strip}" }
      @wait_for_ready = wait_for_ready
      @log = logger
      @closed = false
    end

    attr_reader :wait_for_ready

    def close
      @closed = true
      @stub.instance_variable_get(:@ch)&.close
      nil
    end

    def closed? = @closed

    # ---- workflows & instances ----

    # Register a workflow definition (a FlowSpec, or a Graph which is compiled first); returns its
    # version. Idempotent for the same graph. +force+ replaces an already-registered (name, version)
    # whose graph differs instead of failing; the server refuses it unless configured to allow graph
    # replacement.
    def register(flow_spec, force: false)
      flow_spec = flow_spec.compile if flow_spec.is_a?(Graph)
      req = PB::WorkflowDefinition.new(definition: Convert.to_struct(flow_spec.definition), force: force)
      Integer(call(:register_workflow, req, ready: true).version)
    end

    # The registered graph for +name+ (the latest version, or +version+) as a plain Hash -- the
    # server's source of truth for a workflow's step names, kinds, and queues. Raises GRPC::NotFound if
    # it was never registered. Used by worker reconciliation.
    def get_workflow(name, version: nil)
      req = PB::GetWorkflowRequest.new(name: name)
      req.version = version unless version.nil?
      Convert.from_struct(call(:get_workflow, req, ready: true).definition)
    end

    # The names of every registered workflow.
    def workflow_names = call(:list_workflows, PB::Empty.new).workflows.to_a

    # Start an instance of +workflow+ (a FlowSpec, Graph, or name); returns its id.
    def start(workflow, context = {}, version: nil, correlation_id: nil)
      req = PB::StartInstanceRequest.new(workflow: workflow_name(workflow), context: Convert.to_value(context))
      req.version = version unless version.nil?
      req.correlation_id = correlation_id unless correlation_id.nil?
      call(:start_instance, req).instance_id
    end

    def instance(instance_id)
      view(call(:get_instance, PB::InstanceIdRequest.new(instance_id: instance_id)).instance)
    end

    # Poll until the instance reaches a terminal state, or raise Wiggle::TimeoutError.
    def await_completion(instance_id, timeout: 30, poll_interval: 0.2)
      deadline = monotonic + timeout
      loop do
        v = instance(instance_id)
        return v if v.done?
        raise TimeoutError, "instance #{instance_id} still #{v.status} after #{timeout}s" if monotonic >= deadline

        Kernel.sleep(poll_interval)
      end
    end

    def list_instances(workflow: nil, status: nil, correlation_id: nil, limit: 100)
      req = PB::ListInstancesRequest.new(limit: limit)
      req.workflow = workflow unless workflow.nil?
      req.status = status unless status.nil?
      req.correlation_id = correlation_id unless correlation_id.nil?
      call(:list_instances, req).instances.map { |v| view(v) }
    end

    def cancel(instance_id, reason: "cancelled")
      call(:cancel_instance, PB::CancelInstanceRequest.new(instance_id: instance_id, reason: reason))
      nil
    end

    # Deliver a named signal; +payload+ merges into the instance context.
    def signal(instance_id, signal, payload = nil)
      call(:signal_instance, PB::SignalRequest.new(instance_id: instance_id, signal: signal,
                                                   payload: Convert.to_value(payload)))
      nil
    end

    # ---- schedules ----

    # Start +workflow+ on a cadence: pass exactly one of +every+ (seconds) or +cron+ (five-field, UTC).
    def create_schedule(workflow, every: nil, cron: nil, context: nil)
      raise ArgumentError, "pass exactly one of every: or cron:" if every.nil? == cron.nil?

      req = PB::CreateScheduleRequest.new(workflow: workflow_name(workflow), context: Convert.to_value(context))
      if every.nil?
        req.cron = cron
      else
        req.every_millis = (every * 1000).to_i
      end
      schedule(call(:create_schedule, req))
    end

    def list_schedules = call(:list_schedules, PB::Empty.new).schedules.map { |s| schedule(s) }

    def delete_schedule(schedule_id)
      call(:delete_schedule, PB::ScheduleIdRequest.new(id: schedule_id))
      nil
    end

    # ---- cluster / health ----

    def health
      h = call(:health_check, PB::Empty.new)
      { "status" => h.status, "node" => h.node, "leader" => h.leader }
    end

    def cluster
      c = call(:get_cluster, PB::Empty.new)
      {
        "self" => c.self, "leader" => c.leader,
        "members" => c.members.map do |m|
          { "id" => m.id, "name" => m.name, "workers" => m.workers, "leader" => m.leader, "alive" => m.alive }
        end
      }
    end

    # ---- event feed ----

    # Long-poll the event log through the named cursor +consumer+; the first poll registers it at
    # +start_from+ (0 = tail, -1 = earliest retained, else a seq). Events stay unacknowledged -- and
    # are delivered again -- until #ack_events moves the cursor past them.
    def poll_events(consumer, max_events: 100, wait: 10, start_from: 0)
      res = call(:poll_events, PB::PollEventsRequest.new(consumer: consumer, max: max_events,
                                                         wait_millis: (wait * 1000).to_i, start_from: start_from))
      EventBatch.new(events: res.events.map { |e| event(e) }, retry_after_millis: res.retry_after_millis)
    end

    # Acknowledge every event up to and including the one whose +cursor+ is given (or, on a
    # single-shard log only, every event with +seq+ at most this).
    def ack_events(consumer, cursor: nil, seq: nil)
      raise ArgumentError, "pass exactly one of cursor: or seq:" if cursor.nil? == seq.nil?

      req = PB::AckEventsRequest.new(consumer: consumer)
      if cursor.nil?
        req.acked_seq = seq
      else
        req.acked_cursor = cursor
      end
      call(:ack_events, req)
      nil
    end

    # ---- search, statistics, backlog ----

    # Search instances by the words of their correlation id, context, termination reason or error --
    # or by meaning with +semantic+. GRPC::FailedPrecondition when search is not enabled.
    def search(text = "", workflow: nil, status: nil, updated_from: 0, updated_to: 0, limit: 50,
               partial_ok: false, semantic: false)
      req = PB::SearchRequest.new(text: text, updated_from: updated_from, updated_to: updated_to, limit: limit,
                                  partial_ok: partial_ok, semantic: semantic)
      req.workflow = workflow unless workflow.nil?
      req.status = status unless status.nil?
      res = call(:search_instances, req)
      hits = res.hits.map do |h|
        SearchHit.new(instance_id: h.instance_id, workflow: h.workflow, version: h.version, status: h.status,
                      correlation_id: h.correlation_id, created_at: h.created_at, updated_at: h.updated_at,
                      score: h.score, purged: h.purged)
      end
      SearchResult.new(hits: hits, partial: res.partial, model: res.model)
    end

    # Per-step duration and queue-wait percentiles over recently settled steps of one workflow version
    # (0 = the latest).
    def step_stats(workflow, version: 0, since: 0, sample: 0)
      res = call(:get_step_stats, PB::StepStatsRequest.new(workflow: workflow, version: version, since: since,
                                                           sample: sample))
      nodes = res.nodes.map do |n|
        NodeStats.new(node_id: n.node_id, name: n.name, count: n.count, mean_millis: n.mean_millis,
                      p50_millis: n.p50_millis, p95_millis: n.p95_millis, max_millis: n.max_millis,
                      wait_p50_millis: n.wait_p50_millis, wait_p95_millis: n.wait_p95_millis)
      end
      StepStats.new(workflow: res.workflow, version: res.version, nodes: nodes)
    end

    # Dispatchable work by (workflow, version, queue), each slice saying whether any worker polling
    # this node would claim it.
    def backlog_coverage(max_slices: 0)
      res = call(:get_backlog_coverage, PB::BacklogCoverageRequest.new(max: max_slices))
      slices = res.slices.map do |b|
        BacklogSlice.new(workflow: b.workflow, version: b.version, queue: b.queue, ready_count: b.ready_count,
                         oldest_available_at: b.oldest_available_at, covered: b.covered, coverage: b.coverage,
                         claimable: b.has_claimable? ? b.claimable : nil, criteria: b.criteria.to_a)
      end
      BacklogCoverage.new(slices: slices, live_pollers: res.live_pollers)
    end

    # ---- low-level worker RPCs (used by Worker) ----

    # Claim up to +max_tasks+ ready tasks on +queues+. A non-empty +versions+ -- [workflow, version]
    # pairs -- claims only those versions' tasks. Returns the raw PB::TaskList.
    def poll(worker_id, queues, max_tasks, lease_millis, wait_millis, versions: [])
      req = PB::PollRequest.new(
        worker_id: worker_id, queues: queues.to_a, max: max_tasks, lease_millis: lease_millis,
        wait_millis: wait_millis,
        versions: versions.map { |w, v| PB::WorkflowVersion.new(workflow: w, version: v) }
      )
      # the long poll may legitimately hold the call for wait_millis; allow for it plus slack
      call(:poll_tasks, req, ready: true, deadline: Time.now + (wait_millis / 1000.0) + 30)
    end

    # Report the finished steps of one leased task. +final+ says this worker takes no continuation.
    # Raises ReportRefused when the server refuses the run.
    def report_steps(task_id, lease_owner, steps, final: true)
      run = PB::ReportedRun.new(task_id: task_id, lease_owner: lease_owner, final: final,
                                steps: steps.map { |s| step_result(s) })
      res = call(:report_steps, PB::ReportStepsRequest.new(runs: [run]), ready: true)
      r = res.results.find { |x| x.task_id == task_id }
      raise ReportRefused.new(task_id, 500, "no answer for this run") if r.nil?
      raise ReportRefused.new(task_id, r.error_status, r.error) if r.error_status != 0 || r.applied.nil?

      a = r.applied
      RunApplied.new(instance_status: a.instance_status, lease_expires_at: a.lease_expires_at,
                     next_task_id: a.next_task_id.empty? ? nil : a.next_task_id)
    end

    def fail(task_id, lease_owner, message, retryable:)
      call(:fail_task, PB::TaskFailureRequest.new(task_id: task_id, lease_owner: lease_owner, message: message,
                                                  retryable: retryable), ready: true)
      nil
    end

    def heartbeat(task_id, lease_owner, extend_millis)
      call(:heartbeat_task, PB::HeartbeatRequest.new(task_id: task_id, lease_owner: lease_owner,
                                                     extend_millis: extend_millis), ready: true).lease_expires_at
    end

    private

    # One unary call. Every failure is logged here, in one place (method, status code, details);
    # CANCELLED (worker shutdown) is skipped. With +ready+ and wait_for_ready on, UNAVAILABLE is
    # retried with a capped backoff until the server is reachable (or the client is closed) -- Ruby
    # gRPC has no per-call wait_for_ready, so the client provides it.
    def call(method, req, ready: false, deadline: nil)
      backoff = 0.1
      begin
        opts = { metadata: @metadata }
        opts[:deadline] = deadline unless deadline.nil?
        @stub.public_send(method, req, **opts)
      rescue GRPC::Unavailable => e
        raise unless ready && @wait_for_ready && !@closed

        @log.debug { "rpc #{method} waiting for the server: #{e.details}" }
        Kernel.sleep(backoff)
        backoff = [backoff * 2, 2.0].min
        retry
      rescue GRPC::Cancelled
        raise
      rescue GRPC::BadStatus => e
        level = EXPECTED_FAILURES.any? { |k| e.is_a?(k) } ? :debug : :warn
        @log.public_send(level) { "rpc #{method} failed: #{e.class.name.split('::').last}: #{e.details}" }
        raise
      end
    end

    def strip_scheme(target)
      i = target.index("://")
      i.nil? ? target : target[(i + 3)..]
    end

    def workflow_name(workflow)
      case workflow
      when FlowSpec, Graph then workflow.name
      else workflow.to_s
      end
    end

    def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    def step_result(s)
      r = PB::StepResult.new(
        node_id: s.node_id, started_at: s.started_at, finished_at: s.finished_at,
        events: s.events.map { |type, payload| PB::EmittedEvent.new(type: type, payload: Convert.to_value(payload)) }
      )
      if !s.predicate.nil?
        r.predicate_value = s.predicate
      elsif !s.merge.nil?
        r.merge = Convert.to_value(s.merge)
      end
      r
    end

    def view(v)
      InstanceView.new(
        id: v.id, workflow: v.workflow, version: v.version, status: v.status,
        termination_reason: v.has_termination_reason? ? v.termination_reason : nil,
        error: v.has_error? ? v.error : nil,
        context: Convert.from_value(v.context), created_at: v.created_at, updated_at: v.updated_at
      )
    end

    def schedule(s)
      ScheduleView.new(id: s.id, workflow: s.workflow, every_millis: s.every_millis, cron: s.cron,
                       next_fire_at: s.next_fire_at, created_at: s.created_at)
    end

    def event(e)
      EventView.new(seq: e.seq, shard: e.shard, instance_id: e.instance_id, workflow: e.workflow, version: e.version,
                    correlation_id: e.correlation_id, type: e.type, created_at: e.created_at,
                    payload: Convert.from_value(e.payload), node_id: e.node_id, cursor: e.cursor)
    end
  end

  # The client is also reachable under the Java/Python name.
  WiggleClient = Client
end
