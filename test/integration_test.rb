# frozen_string_literal: true

require_relative "test_helper"

# Opt-in end-to-end tests against a running server. Set WIGGLE_TEST_URL, e.g. after starting the
# engine image:  docker run --rm -p 8080:8080 hadielmougy/wiggle
#
#   WIGGLE_TEST_URL=localhost:8080 bundle exec rake test
class IntegrationTest < Minitest::Test
  include Wiggle

  URL = ENV.fetch("WIGGLE_TEST_URL", nil)

  def setup
    skip "set WIGGLE_TEST_URL to run integration tests" unless URL
    @client = Client.new(URL)
  end

  def teardown
    @client&.close
  end

  def unique(prefix) = "#{prefix}-#{Process.clock_gettime(Process::CLOCK_REALTIME, :nanosecond)}"

  def new_worker(id) = Worker.new(@client, id, concurrency: 2, long_poll_wait: 1)

  def run_flow(flow, worker, context, timeout: 30)
    @client.register(flow)
    worker.start { @client.await_completion(@client.start(flow, context), timeout: timeout) }
  end

  def test_linear_flow_with_gate
    name = unique("rb-it-order")
    flow = Wiggle.workflow(name) do
      step "validate"
      gate "in-stock"
      step "charge", queue: "#{name}-payments"
    end
    w = new_worker("it-order")
        .handle(name, "validate") { |c| c.merge("status" => "VALIDATED") }
        .handle_gate(name, "in-stock") { |c| c["qty"].positive? }
        .handle(name, "charge") { |c| c.merge("paymentRef" => "auth-#{c['orderId']}") }
    @client.register(flow)
    w.start do
      ok = @client.await_completion(@client.start(flow, { "orderId" => "x1", "qty" => 1 }))
      assert_equal "COMPLETED", ok.status, ok.error
      assert_equal({ "orderId" => "x1", "qty" => 1, "status" => "VALIDATED", "paymentRef" => "auth-x1" }, ok.context)
      gated = @client.await_completion(@client.start(flow, { "orderId" => "x2", "qty" => 0 }))
      assert_equal %w[COMPLETED gated:in-stock], [gated.status, gated.termination_reason]
    end
  end

  def test_fork_combine
    name = unique("rb-it-fork")
    flow = Graph.new(name, [Fork.new([Branch.new("left", [Step.new("l")]), Branch.new("right", [Step.new("r")])],
                                     combine: "join")]).compile
    base_seen = nil
    w = new_worker("it-fork")
        .handle(name, "l") { |c| c.merge("side" => "L") }
        .handle(name, "r") { |c| c.merge("side" => "R") }
        .handle_combine(name, "join") do |c|
          base_seen = Current.base
          { "n" => c["n"], "sides" => c["left"]["side"] + c["right"]["side"] }
        end
    v = run_flow(flow, w, { "n" => 1 })
    assert_equal "COMPLETED", v.status, v.error
    assert_equal({ "n" => 1, "sides" => "LR" }, v.context)
    assert_equal({ "n" => 1 }, base_seen)
  end

  def test_for_each
    name = unique("rb-it-each")
    flow = Wiggle.workflow(name) do
      for_each("doubled", over: "xs", combine: "total") { step "double" }
    end
    w = new_worker("it-each")
        .handle(name, "double") { |x| x * Current.base["factor"] }
        .handle_combine(name, "total") { |c| { "total" => c["doubled"].sum } }
    v = run_flow(flow, w, { "xs" => [1, 2, 3], "factor" => 2 })
    assert_equal "COMPLETED", v.status, v.error
    assert_equal({ "total" => 12 }, v.context)
  end

  def test_choose_and_do_while
    name = unique("rb-it-loop")
    flow = Wiggle.workflow(name) do
      do_while("more", max_iterations: 10) { step "inc" }
      choose do
        on("big") { step "label-big" }
        otherwise { step "label-small" }
      end
    end
    w = new_worker("it-loop")
        .handle(name, "inc") { |c| c.merge("i" => c["i"] + 1) }
        .handle_gate(name, "more") { |c| c["i"] < 4 }
        .handle_gate(name, "big") { |c| c["i"] > 3 }
        .handle(name, "label-big") { |c| c.merge("label" => "big") }
        .handle(name, "label-small") { |c| c.merge("label" => "small") }
    v = run_flow(flow, w, { "i" => 0 })
    assert_equal "COMPLETED", v.status, v.error
    assert_equal({ "i" => 4, "label" => "big" }, v.context)
  end

  def test_signal_and_sleep
    name = unique("rb-it-signal")
    flow = Wiggle.workflow(name) do
      sleep "nap", millis: 100
      await_signal "approve", timeout: 30
      step "finish"
    end
    @client.register(flow)
    w = new_worker("it-signal").handle(name, "finish") { |c| c.merge("done" => true) }
    v = w.start do
      id = @client.start(flow, {})
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
      begin
        @client.signal(id, "approve", { "by" => "ops" })
      rescue GRPC::BadStatus # not waiting for the signal yet
        raise if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

        Kernel.sleep 0.1
        retry
      end
      @client.await_completion(id, timeout: 20)
    end
    assert_equal "COMPLETED", v.status, v.error
    assert_equal({ "by" => "ops", "done" => true }, v.context)
  end

  def test_sub_workflow
    child = unique("rb-it-child")
    parent = unique("rb-it-parent")
    child_flow = Wiggle.workflow(child) { step "work" }
    @client.register(child_flow)
    flow = Wiggle.workflow(parent) do
      sub_workflow "child", child_flow
      step "after"
    end
    w = new_worker("it-sub")
        .handle(child, "work") { |c| c.merge("child" => true) }
        .handle(parent, "after") { |c| c.merge("after" => true) }
    v = run_flow(flow, w, {})
    assert_equal "COMPLETED", v.status, v.error
    assert_equal({ "child" => true, "after" => true }, v.context)
  end

  def test_compensation
    name = unique("rb-it-saga")
    flow = Wiggle.workflow(name) do
      step "reserve", compensate: true
      step "overwrite"
      step "boom"
    end
    undone = Queue.new
    w = new_worker("it-saga")
        .handle(name, "reserve") { |c| { "orderId" => c["orderId"], "reservationRef" => "r-1" } }
        .handle_compensation(name, "reserve") { |comp| undone << comp }
        .handle(name, "overwrite") { |_| { "unrelated" => true } }
        .handle(name, "boom") { |_| raise PermanentError, "downstream exploded" }
    v = run_flow(flow, w, { "orderId" => "o-9" })
    assert_equal "COMPENSATED", v.status, v.error
    comp = undone.pop
    assert_equal "r-1", comp.result["reservationRef"]
    assert_equal({ "orderId" => "o-9" }, comp.input)
  end

  def test_register_handlers_object
    name = unique("rb-it-typed")
    flow = Wiggle.workflow(name) do
      step "validate"
      gate "in-stock"
      effect "notify"
    end
    notified = Queue.new
    handlers = Class.new(Wiggle::Handlers) do
      define_method(:validate) { |o| o.merge("status" => "VALIDATED") }
      define_method(:in_stock?) { |o| o["qty"].positive? }
      define_method(:notify) { |o| notified << o["orderId"] }
      effect :notify
    end
    w = new_worker("it-typed").register_handlers(handlers.new, workflow: name)
    v = run_flow(flow, w, { "orderId" => "t-1", "qty" => 2 })
    assert_equal "COMPLETED", v.status, v.error
    assert_equal({ "orderId" => "t-1", "qty" => 2, "status" => "VALIDATED" }, v.context)
    assert_equal "t-1", notified.pop
  end

  def test_retry_then_success_reports_the_attempt
    name = unique("rb-it-retry")
    flow = Wiggle.workflow(name) { step "flaky", retry: Retry.fixed(5, 0.1) }
    w = new_worker("it-retry").handle(name, "flaky") do |_|
      raise "blip" if Current.attempt < 2

      { "attempt" => Current.attempt, "step" => Current.step_name }
    end
    v = run_flow(flow, w, {}, timeout: 40)
    assert_equal "COMPLETED", v.status, v.error
    assert_equal({ "attempt" => 2, "step" => "flaky" }, v.context)
  end

  def test_emitted_events_reach_the_feed_and_timings_feed_step_stats
    name = unique("rb-it-events")
    consumer = "#{name}-feed"
    @client.poll_events(consumer, max_events: 1, wait: 0, start_from: -1) # register the cursor
    flow = Wiggle.workflow(name) { step "ship" }
    w = new_worker("it-events").handle(name, "ship") do |c|
      Current.emit("order.shipped", { "attempt" => Current.attempt, "step" => Current.step_name })
      c
    end
    v = run_flow(flow, w, { "id" => "o-1" })
    assert_equal "COMPLETED", v.status, v.error

    seen = {}
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
    while seen.size < 3 && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      batch = @client.poll_events(consumer, max_events: 100, wait: 2)
      batch.events.select { |e| e.instance_id == v.id }.each { |e| seen[e.type] = e }
      @client.ack_events(consumer, cursor: batch.events.last.cursor) unless batch.events.empty?
    end
    assert_empty %w[wf.started order.shipped wf.completed] - seen.keys, seen.keys.inspect
    assert_equal({ "attempt" => 1, "step" => "ship" }, seen["order.shipped"].payload)
    refute_empty seen["order.shipped"].node_id

    assert_equal [["ship", 1]], @client.step_stats(name).nodes.map { |n| [n.name, n.count] }
  end

  def test_queries
    name = unique("rb-it-query")
    flow = Wiggle.workflow(name) { step "a" }
    v = run_flow(flow, new_worker("it-query").handle(name, "a") { |c| c }, { "k" => 1 })
    assert_includes @client.workflow_names, name
    assert_equal "a", @client.get_workflow(name)["nodes"].find { |n| n["kind"] == "TASK" }["name"]
    assert_equal [v.id], @client.list_instances(workflow: name).map(&:id)
    refute_empty @client.health["status"]
  end
end
