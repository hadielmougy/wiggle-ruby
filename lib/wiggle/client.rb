# frozen_string_literal: true

require "grpc"
require_relative "proto/wiggle_services_pb"

module Wiggle
  # @api private
  PB = ::Com::Wiggle::Proto

  # Statuses an instance never leaves. COMPENSATING is NOT terminal: the reverse pass is still running.
  TERMINAL_STATUSES = %w[COMPLETED FAILED CANCELLED COMPENSATED COMPENSATION_FAILED].freeze

  InstanceView = Struct.new(:id, :workflow, :version, :status, :termination_reason, :error, :context,
                            :created_at, :updated_at) do
    def terminal? = TERMINAL_STATUSES.include?(status)
  end

  # An instance plus the tokens driving it (WiggleClient#instance_detail).
  InstanceWithTokens = Struct.new(:instance, :tokens)

  # A token as carried on the wire. +started_at+/+finished_at+ are nil for an untimed step;
  # +input+/+output+ are the step's recorded JSON, nil where none was recorded.
  TokenInfo = Struct.new(:id, :node_id, :kind, :status, :activity, :attempt, :available_at, :lease_owner,
                         :last_error, :started_at, :finished_at, :created_at, :input, :output)

  ScheduleInfo = Struct.new(:id, :workflow, :every_millis, :cron, :next_fire_at, :created_at)

  # One entry of the event log: a lifecycle transition (+wf.*+) or a handler-emitted event.
  EventView = Struct.new(:seq, :instance_id, :workflow, :version, :correlation_id, :type, :created_at, :payload,
                         :node_id, :shard, :cursor)

  # One search hit; +purged+ when the instance is gone and only its document remains.
  SearchHit = Struct.new(:instance_id, :workflow, :version, :status, :correlation_id, :created_at, :updated_at,
                         :score, :purged)
  SearchResult = Struct.new(:hits, :partial, :model)

  NodeStats = Struct.new(:node_id, :name, :count, :mean_millis, :p50_millis, :p95_millis, :max_millis,
                         :wait_p50_millis, :wait_p95_millis)

  BacklogSlice = Struct.new(:workflow, :version, :queue, :ready_count, :oldest_available_at, :covered,
                            :live_pollers, :coverage, :claimable, :criteria)

  # What a poll claimed: the leased tasks, and the server's backpressure hold-off (0 = none).
  PollResult = Struct.new(:tasks, :retry_after_millis)

  # One finished step, as a worker reports it: exactly one of +merge+ (a task's complete next
  # context; nil leaves it unchanged) or +predicate_value+ (a predicate's branch).
  StepReport = Struct.new(:node_id, :merge, :predicate_value, :started_at, :finished_at, :events) do
    def initialize(node_id, merge = nil, predicate_value = nil, started_at = nil, finished_at = nil, events = [])
      super
    end
  end

  # The steps of one leased task, reported together. +final_handback+ says the worker takes no
  # continuation.
  RunSubmission = Struct.new(:task_id, :lease_owner, :steps, :final_handback)

  # What a reported run applied to (+outcome+), or why it was refused (+error_status+, +error+).
  RunOutcome = Struct.new(:outcome, :error_status, :error) do
    def ok? = !outcome.nil?
  end

  # The applied side of a RunOutcome. Anything but RUNNING means stop here.
  ReportResult = Struct.new(:instance_status, :lease_expires_at, :next_task_id)

  # The gRPC control-plane client: register workflows, start and track instances, deliver signals,
  # manage schedules, read the event feed. Also carries the worker RPCs (poll / report_steps / fail /
  # heartbeat) a Worker uses.
  #
  #   client = Wiggle::WiggleClient.new("localhost:8080")
  #   client.register(order)
  #   id = client.start(order, { "orderId" => "A-1" })
  #   client.await_completion(id, 30).status   # => "COMPLETED"
  #   client.close
  #
  # Every call that fails raises WiggleApiException. A call the server could not be reached for
  # (UNAVAILABLE) is retried with exponential backoff first -- WIGGLE_RPC_MAX_ATTEMPTS times (default
  # 5; 1 disables retry), starting at WIGGLE_RPC_RETRY_DELAY_MILLIS (default 200).
  class WiggleClient
    # Connects to +target+ (+host:port+; an +http://+ style scheme prefix is stripped).
    #
    # +tls+: a GRPC::Core::ChannelCredentials (your CA, and a client key/cert for mTLS), or +true+
    # for the system trust roots; nil connects in plaintext. +api_key+ (default: ENV["WIGGLE_API_KEY"])
    # is presented on every call as +authorization: Bearer <key>+; a key travels in the clear on a
    # plaintext channel, so use one with TLS.
    def initialize(target, tls: nil, api_key: ENV.fetch("WIGGLE_API_KEY", nil))
      creds = case tls
              when nil, false then :this_channel_is_insecure
              when true then GRPC::Core::ChannelCredentials.new
              else tls
              end
      @stub = PB::WiggleControlPlane::Stub.new(strip_scheme(target), creds)
      @metadata = api_key.nil? || api_key.strip.empty? ? {} : { "authorization" => "Bearer #{api_key.strip}" }
      @max_attempts = int_env("WIGGLE_RPC_MAX_ATTEMPTS", 5)
      @retry_delay = int_env("WIGGLE_RPC_RETRY_DELAY_MILLIS", 200) / 1000.0
    end

    def close
      @stub.instance_variable_get(:@ch)&.close
      nil
    end

    # ---- workflows & instances ----

    # Registers a workflow topology. Idempotent for the same graph. +force+ replaces an already
    # registered (name, version) whose graph differs instead of failing (409); the server refuses it
    # unless configured to allow graph replacement.
    def register(flow_spec, force = false)
      call(:register_workflow, PB::WorkflowDefinition.new(definition: Convert.to_struct(flow_spec.definition), force: force))
      nil
    end

    # The registered graph for +name+ (+version+ nil = the latest) as a Hash -- the server's source of
    # truth for a workflow's step names, kinds, and queues. 404 if it was never registered.
    def get_workflow(name, version = nil)
      req = PB::GetWorkflowRequest.new(name: name)
      req.version = version unless version.nil?
      Convert.from_struct(call(:get_workflow, req).definition)
    end

    # The names of all registered workflows.
    def workflow_names = call(:list_workflows, PB::Empty.new).workflows.to_a

    # Starts an instance; returns its id. A FlowSpec starts its own version; a name starts the latest
    # unless +version+ is given.
    def start(workflow, context, version = nil, correlation_id = nil)
      if workflow.is_a?(FlowSpec)
        version ||= workflow.version
        workflow = workflow.name
      end
      req = PB::StartInstanceRequest.new(workflow: workflow)
      req.context = Convert.to_value(context) unless context.nil?
      req.version = version unless version.nil?
      req.correlation_id = correlation_id unless correlation_id.nil?
      call(:start_instance, req).instance_id
    end

    def instance(instance_id) = view(call(:get_instance, PB::InstanceIdRequest.new(instance_id: instance_id)).instance)

    # An instance plus its tokens (#instance drops the tokens).
    def instance_detail(instance_id)
      detail = call(:get_instance, PB::InstanceIdRequest.new(instance_id: instance_id))
      InstanceWithTokens.new(view(detail.instance), detail.tokens.map { |t| token_info(t) })
    end

    # Polls until the instance is terminal; raises Timeout::Error after +timeout+ seconds.
    def await_completion(instance_id, timeout)
      deadline = monotonic + timeout
      v = instance(instance_id)
      until v.terminal?
        raise Timeout::Error, "instance #{instance_id} still #{v.status} after #{timeout}s" if monotonic > deadline

        Kernel.sleep(0.05)
        v = instance(instance_id)
      end
      v
    end

    # Instances filtered by +workflow+ and/or +status+ (either may be nil), newest first.
    def list_instances(workflow, status, limit)
      req = PB::ListInstancesRequest.new(limit: limit)
      req.workflow = workflow unless workflow.nil?
      req.status = status unless status.nil?
      call(:list_instances, req).instances.map { |v| view(v) }
    end

    # Instances started with +correlation_id+ (a business key), newest first.
    def find_by_correlation(correlation_id, limit = 50)
      call(:list_instances, PB::ListInstancesRequest.new(correlation_id: correlation_id, limit: limit))
        .instances.map { |v| view(v) }
    end

    def cancel(instance_id, reason)
      call(:cancel_instance, PB::CancelInstanceRequest.new(instance_id: instance_id, reason: reason.to_s))
      nil
    end

    # Delivers a named signal to a running instance; +payload+ merges into its context.
    def signal(instance_id, signal, payload)
      req = PB::SignalRequest.new(instance_id: instance_id, signal: signal)
      req.payload = Convert.to_value(payload) unless payload.nil?
      call(:signal_instance, req)
      nil
    end

    # ---- schedules ----

    # Creates a recurring start every +every+ seconds; returns the schedule id.
    def create_schedule(workflow, every, context)
      req = PB::CreateScheduleRequest.new(workflow: workflow, every_millis: (every * 1000).round)
      req.context = Convert.to_value(context) unless context.nil?
      call(:create_schedule, req).id
    end

    # Creates a recurring start on a five-field cron expression (evaluated in UTC); returns its id.
    def create_cron_schedule(workflow, cron, context)
      req = PB::CreateScheduleRequest.new(workflow: workflow, cron: cron)
      req.context = Convert.to_value(context) unless context.nil?
      call(:create_schedule, req).id
    end

    # All schedules. +every_millis+ is 0 for cron schedules; +cron+ is nil for interval ones.
    def schedules
      call(:list_schedules, PB::Empty.new).schedules.map do |s|
        ScheduleInfo.new(s.id, s.workflow, s.every_millis, s.cron.empty? ? nil : s.cron, s.next_fire_at, s.created_at)
      end
    end

    def delete_schedule(id)
      call(:delete_schedule, PB::ScheduleIdRequest.new(id: id))
      nil
    end

    # ---- cluster, search, statistics, backlog ----

    def cluster
      c = call(:get_cluster, PB::Empty.new)
      { "self" => c.self, "leader" => c.leader,
        "members" => c.members.map do |m|
          { "id" => m.id, "name" => m.name, "firstHeartbeat" => m.first_heartbeat, "lastHeartbeat" => m.last_heartbeat,
            "workers" => m.workers, "leader" => m.leader, "alive" => m.alive }
        end }
    end

    # Instances whose correlation id, context, termination reason or error contain the words of
    # +text+ -- or, with +semantic+, those closest to it in meaning. +updated_from+/+updated_to+ are
    # epoch-millis bounds (nil for none). With +partial_ok+, a shard that does not answer leaves the
    # result partial instead of failing it. 409 when search is not enabled.
    def search(text, workflow, status, updated_from, updated_to, limit, partial_ok, semantic = false)
      req = PB::SearchRequest.new(text: text.to_s, limit: limit, partial_ok: partial_ok, semantic: semantic)
      req.workflow = workflow unless workflow.nil?
      req.status = status unless status.nil?
      req.updated_from = updated_from unless updated_from.nil?
      req.updated_to = updated_to unless updated_to.nil?
      r = call(:search_instances, req)
      hits = r.hits.map do |h|
        SearchHit.new(h.instance_id, h.workflow, h.version, h.status, h.correlation_id.empty? ? nil : h.correlation_id,
                      h.created_at, h.updated_at, h.score, h.purged)
      end
      SearchResult.new(hits, r.partial, r.model.empty? ? nil : r.model)
    end

    # Per-step duration and queue-wait percentiles over recently settled steps of one workflow
    # version (+version+ nil = the latest). Slowest 95th percentile first.
    def step_stats(workflow, version, since, sample)
      req = PB::StepStatsRequest.new(workflow: workflow, since: since, sample: sample)
      req.version = version unless version.nil?
      call(:get_step_stats, req).nodes
                                .map do |n|
        NodeStats.new(n.node_id, n.name, n.count, n.mean_millis, n.p50_millis, n.p95_millis, n.max_millis,
                      n.wait_p50_millis, n.wait_p95_millis)
      end.sort_by { |n| -n.p95_millis }
    end

    # Dispatchable work by (workflow, version, queue), each slice saying whether any worker polling
    # this node would claim it.
    def backlog_coverage(max)
      res = call(:get_backlog_coverage, PB::BacklogCoverageRequest.new(max: max))
      res.slices.map do |b|
        BacklogSlice.new(b.workflow, b.version, b.queue, b.ready_count, b.oldest_available_at, b.covered,
                         res.live_pollers, b.coverage, b.has_claimable? ? b.claimable : nil, b.criteria.to_a)
      end
    end

    # ---- event feed ----

    # The next entries of the event log for +consumer+, oldest first, long-polling up to
    # +wait_millis+. A consumer is a named cursor: its first poll registers it at the tail
    # (+start_from+ 0), at the earliest retained entry (-1), or after the seq it names. Delivery is
    # at-least-once and the cursor only moves on #ack_events.
    def poll_events(consumer, max, wait_millis, start_from)
      res = call(:poll_events, PB::PollEventsRequest.new(consumer: consumer, max: max, wait_millis: wait_millis,
                                                         start_from: start_from))
      res.events.map do |e|
        EventView.new(e.seq, e.instance_id, e.workflow, e.version, e.correlation_id.empty? ? nil : e.correlation_id,
                      e.type, e.created_at, Convert.from_value(e.payload), e.node_id.empty? ? nil : e.node_id,
                      e.shard, e.cursor)
      end
    end

    # Acknowledges, for +consumer+, every event up to and including the one whose cursor (a String)
    # is given -- or, on a log that is not sharded, every event up to a seq (an Integer).
    def ack_events(consumer, cursor_or_seq)
      req = PB::AckEventsRequest.new(consumer: consumer)
      if cursor_or_seq.is_a?(Integer)
        req.acked_seq = cursor_or_seq
      else
        req.acked_cursor = cursor_or_seq.to_s
      end
      call(:ack_events, req)
      nil
    end

    # ---- the worker RPCs (used by Worker) ----

    # Claims up to +max+ ready tasks on +queues+. A non-empty +versions+ ([workflow, version] pairs)
    # claims only those versions' tasks.
    def poll(worker_id, queues, max, lease_millis, wait_millis, versions = [])
      req = PB::PollRequest.new(worker_id: worker_id, queues: queues.to_a, max: max, lease_millis: lease_millis,
                                wait_millis: wait_millis,
                                versions: versions.map { |w, v| PB::WorkflowVersion.new(workflow: w, version: v) })
      res = call(:poll_tasks, req, deadline: Time.now + (wait_millis / 1000.0) + 30)
      PollResult.new(res.tasks.to_a, res.retry_after_millis)
    end

    # Reports finished steps, one RunSubmission per leased task; returns each run's RunOutcome by
    # task id. A refused run wrote nothing.
    def report_steps(runs)
      req = PB::ReportStepsRequest.new(runs: runs.map do |r|
        PB::ReportedRun.new(task_id: r.task_id, lease_owner: r.lease_owner, final: r.final_handback,
                            steps: r.steps.map { |s| step_result(s) })
      end)
      res = call(:report_steps, req)
      res.results.to_h do |r|
        applied = r.applied && ReportResult.new(r.applied.instance_status, r.applied.lease_expires_at,
                                                r.applied.next_task_id.empty? ? nil : r.applied.next_task_id)
        [r.task_id, r.error_status.zero? && applied ? RunOutcome.new(applied, 0, nil) : RunOutcome.new(nil, r.error_status, r.error)]
      end
    end

    def fail(task_id, lease_owner, message, retryable)
      call(:fail_task, PB::TaskFailureRequest.new(task_id: task_id, lease_owner: lease_owner, message: message.to_s,
                                                  retryable: retryable))
      nil
    end

    def heartbeat(task_id, lease_owner, extend_millis)
      call(:heartbeat_task, PB::HeartbeatRequest.new(task_id: task_id, lease_owner: lease_owner, extend_millis: extend_millis))
      nil
    end

    private

    # One unary call, retried on UNAVAILABLE (the RPC almost certainly never ran, so retrying is safe
    # even for non-idempotent operations); every failure becomes a WiggleApiException.
    def call(method, req, deadline: nil)
      attempt = 1
      begin
        opts = { metadata: @metadata }
        opts[:deadline] = deadline unless deadline.nil?
        @stub.public_send(method, req, **opts)
      rescue GRPC::Unavailable => e
        if attempt < @max_attempts
          Kernel.sleep(@retry_delay * (2**(attempt - 1)))
          attempt += 1
          retry
        end
        raise WiggleApiException.new(0, "server unavailable after #{@max_attempts} attempts: #{e.details}"), cause: e
      rescue GRPC::BadStatus => e
        raise WiggleApiException.new(status_code(e), e.details.to_s.empty? ? e.message : e.details), cause: e
      end
    end

    def status_code(e)
      case e
      when GRPC::InvalidArgument then 400
      when GRPC::Unauthenticated then 401
      when GRPC::PermissionDenied then 403
      when GRPC::NotFound then 404
      when GRPC::FailedPrecondition, GRPC::AlreadyExists then 409
      when GRPC::Unavailable, GRPC::DeadlineExceeded then 0
      else 500
      end
    end

    def strip_scheme(target)
      i = target.index("://")
      i.nil? ? target : target[(i + 3)..]
    end

    def int_env(name, default)
      Integer(ENV.fetch(name, ""), exception: false) || default
    end

    def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    def step_result(s)
      r = PB::StepResult.new(node_id: s.node_id, started_at: s.started_at.to_i, finished_at: s.finished_at.to_i,
                             events: (s.events || []).map { |t, p| PB::EmittedEvent.new(type: t, payload: Convert.to_value(p)) })
      if !s.predicate_value.nil?
        r.predicate_value = s.predicate_value
      elsif !s.merge.nil?
        r.merge = Convert.to_value(s.merge)
      end
      r
    end

    def view(v)
      InstanceView.new(v.id, v.workflow, v.version, v.status,
                       v.has_termination_reason? ? v.termination_reason : nil, v.has_error? ? v.error : nil,
                       Convert.from_value(v.context), v.created_at, v.updated_at)
    end

    def token_info(t)
      TokenInfo.new(t.id, t.node_id, t.kind, t.status, t.activity, t.attempt, t.available_at,
                    t.has_lease_owner? ? t.lease_owner : nil, t.has_last_error? ? t.last_error : nil,
                    t.has_started_at? ? t.started_at : nil, t.has_finished_at? ? t.finished_at : nil, t.created_at,
                    t.has_input? ? Convert.from_value(t.input) : nil, t.has_output? ? Convert.from_value(t.output) : nil)
    end
  end
end
