# frozen_string_literal: true

require_relative "test_helper"

# Opt-in end-to-end tests against a running server. Set WIGGLE_TEST_URL, e.g. after starting the
# engine image:  docker run --rm -p 8080:8080 hadielmougy/wiggle
#
#   WIGGLE_TEST_URL=localhost:8080 bundle exec rake test
class IntegrationTest < Minitest::Test
  include Wiggle

  URL = ENV.fetch("WIGGLE_TEST_URL", nil)
  OPTIONS = WorkerOptions.defaults.with_concurrency(2).with_long_poll_wait(1)

  def setup
    skip "set WIGGLE_TEST_URL to run integration tests" unless URL
    @client = WiggleClient.new(URL)
  end

  def teardown
    @client&.close
  end

  def unique(prefix) = "#{prefix}-#{Process.clock_gettime(Process::CLOCK_REALTIME, :nanosecond)}"

  # Registers +spec+, serves it with +handlers+, starts one instance and waits for it.
  def run_flow(spec, handlers, context, timeout = 30)
    @client.register(spec)
    worker = Worker.new(@client, "it-#{spec.name}", OPTIONS).register_handler(spec.name, handlers).start
    begin
      @client.await_completion(@client.start(spec, context), timeout)
    ensure
      worker.close
    end
  end

  def test_linear_flow_with_filter
    name = unique("rb-it-order")
    spec = FlowSpec.define(name, 1) do |f|
      f.then_apply(:validate).then_filter(:in_stock?).then_apply(:charge, "#{name}-payments")
    end
    handlers = Class.new do
      def validate(c) = c.merge("status" => "VALIDATED")
      def in_stock?(c) = c["qty"].positive?
      def charge(c) = c.merge("paymentRef" => "auth-#{c['orderId']}")
    end.new
    @client.register(spec)
    worker = Worker.new(@client, "it-order", OPTIONS).register_handler(name, handlers).start
    begin
      ok = @client.await_completion(@client.start(spec, { "orderId" => "x1", "qty" => 1 }), 30)
      assert_equal "COMPLETED", ok.status, ok.error
      assert_equal({ "orderId" => "x1", "qty" => 1, "status" => "VALIDATED", "paymentRef" => "auth-x1" }, ok.context)
      gated = @client.await_completion(@client.start(spec, { "orderId" => "x2", "qty" => 0 }), 30)
      assert_equal %w[COMPLETED gated:in_stock], [gated.status, gated.termination_reason]
    ensure
      worker.close
    end
  end

  def test_all_of_combine
    spec = FlowSpec.define(unique("rb-it-fork"), 1) do |f|
      Wiggle.all_of(f.then_apply(:left), f.then_apply(:right)).combine(:join)
    end
    seen_base = Queue.new
    handlers = Class.new do
      def left(c) = c.merge("side" => "L")
      def right(c) = c.merge("side" => "R")
      define_method(:join) do |l, r, base|
        seen_base << Wiggle::Step.base
        { "n" => base["n"], "sides" => l["side"] + r["side"] }
      end
    end.new
    v = run_flow(spec, handlers, { "n" => 1 })
    assert_equal "COMPLETED", v.status, v.error
    assert_equal({ "n" => 1, "sides" => "LR" }, v.context)
    assert_equal({ "n" => 1 }, seen_base.pop)
  end

  def test_for_each
    spec = FlowSpec.define(unique("rb-it-each"), 1) do |f|
      f.then_for_each("xs") { |x| x.then_apply(:double) }.combine(:total)
    end
    handlers = Class.new do
      def double(x) = x * Wiggle::Step.base["factor"]
      def total(doubled) = { "total" => doubled.sum }
    end.new
    v = run_flow(spec, handlers, { "xs" => [1, 2, 3], "factor" => 2 })
    assert_equal "COMPLETED", v.status, v.error
    assert_equal({ "total" => 12 }, v.context)
  end

  def test_one_of_and_repeat_while
    spec = FlowSpec.define(unique("rb-it-loop"), 1) do |f|
      counted = f.repeat_while(:more?, 10) { |body| body.then_apply(:inc) }
      Wiggle.one_of(counted.when(:big?).then_apply(:label_big), counted.otherwise.then_apply(:label_small))
    end
    handlers = Class.new do
      def inc(c) = c.merge("i" => c["i"] + 1)
      def more?(c) = c["i"] < 4
      def big?(c) = c["i"] > 3
      def label_big(c) = c.merge("label" => "big")
      def label_small(c) = c.merge("label" => "small")
    end.new
    v = run_flow(spec, handlers, { "i" => 0 })
    assert_equal "COMPLETED", v.status, v.error
    assert_equal({ "i" => 4, "label" => "big" }, v.context)
  end

  def test_await_and_sleep
    spec = FlowSpec.define(unique("rb-it-signal"), 1) { |f| f.then_sleep(0.1).then_await("approve", 30).then_apply(:finish) }
    @client.register(spec)
    handlers = Class.new { def finish(c) = c.merge("done" => true) }.new
    worker = Worker.new(@client, "it-signal", OPTIONS).register_handler(spec.name, handlers).start
    begin
      id = @client.start(spec, {})
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
      begin
        @client.signal(id, "approve", { "by" => "ops" })
      rescue WiggleApiException # not waiting for the signal yet
        raise if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

        Kernel.sleep 0.1
        retry
      end
      v = @client.await_completion(id, 20)
    ensure
      worker.close
    end
    assert_equal "COMPLETED", v.status, v.error
    assert_equal({ "by" => "ops", "done" => true }, v.context)
  end

  def test_sub_flow
    child = FlowSpec.define(unique("rb-it-child"), 1) { |f| f.then_apply(:work) }
    @client.register(child)
    parent = FlowSpec.define(unique("rb-it-parent"), 1) { |f| f.then_sub_flow("child", child).then_apply(:after) }
    @client.register(parent)
    worker = Worker.new(@client, "it-sub", OPTIONS)
                   .register_handler(child.name, Class.new { def work(c) = c.merge("child" => true) }.new)
                   .register_handler(parent.name, Class.new { def after(c) = c.merge("after" => true) }.new)
                   .start
    begin
      v = @client.await_completion(@client.start(parent, {}), 30)
    ensure
      worker.close
    end
    assert_equal "COMPLETED", v.status, v.error
    assert_equal({ "child" => true, "after" => true }, v.context)
  end

  class Reserve
    include Wiggle::CompensableActivity

    def initialize(undone) = @undone = undone
    def execute(c) = { "orderId" => c["orderId"], "reservationRef" => "r-1" }
    def compensate(comp) = @undone << comp
  end

  def test_compensation
    spec = FlowSpec.define(unique("rb-it-saga"), 1) do |f|
      f.then_apply_compensable(:reserve).then_apply(:overwrite).then_apply(:boom)
    end
    undone = Queue.new
    handlers = Class.new do
      define_method(:reserve) { Reserve.new(undone) }
      def overwrite(_c) = { "unrelated" => true }
      def boom(_c) = raise(Wiggle::PermanentActivityException, "downstream exploded")
    end.new
    v = run_flow(spec, handlers, { "orderId" => "o-9" })
    assert_equal "COMPENSATED", v.status, v.error
    comp = undone.pop
    assert_equal "r-1", comp.result["reservationRef"]
    assert_equal({ "orderId" => "o-9" }, comp.input)
  end

  module TypedSteps
    extend Wiggle::ForFlow

    def validate(o) = nil
    def in_stock?(o) = nil
    void def notify(o) = nil
  end

  def test_contract_and_void_effect
    spec = FlowSpec.define(unique("rb-it-typed"), 1, TypedSteps) do |f, s|
      f.then_apply(s.validate).then_filter(s.in_stock?).then_accept(s.notify)
    end
    notified = Queue.new
    handlers = Class.new do
      include TypedSteps
      def validate(o) = o.merge("status" => "VALIDATED")
      def in_stock?(o) = o["qty"].positive?
      define_method(:notify) { |o| notified << o["orderId"] } # returns the queue: void keeps it off the wire
    end.new
    v = run_flow(spec, handlers, { "orderId" => "t-1", "qty" => 2 })
    assert_equal "COMPLETED", v.status, v.error
    assert_equal({ "orderId" => "t-1", "qty" => 2, "status" => "VALIDATED" }, v.context)
    assert_equal "t-1", notified.pop
  end

  def test_retry_then_success_reports_the_attempt
    spec = FlowSpec.define(unique("rb-it-retry"), 1) { |f| f.then_apply(:flaky, RetryPolicy.fixed(5, 0.1)) }
    handlers = Class.new do
      def flaky(_c)
        raise "blip" if Wiggle::Step.attempt < 2

        { "attempt" => Wiggle::Step.attempt, "step" => Wiggle::Step.name }
      end
    end.new
    v = run_flow(spec, handlers, {}, 40)
    assert_equal "COMPLETED", v.status, v.error
    assert_equal({ "attempt" => 2, "step" => "flaky" }, v.context)
  end

  def test_emitted_events_reach_the_feed_and_timings_feed_step_stats
    spec = FlowSpec.define(unique("rb-it-events"), 1) { |f| f.then_apply(:ship) }
    consumer = "#{spec.name}-feed"
    @client.poll_events(consumer, 1, 0, -1) # register the cursor
    handlers = Class.new do
      def ship(c)
        Wiggle::Step.emit("order.shipped", { "attempt" => Wiggle::Step.attempt, "step" => Wiggle::Step.name })
        c
      end
    end.new
    v = run_flow(spec, handlers, { "id" => "o-1" })
    assert_equal "COMPLETED", v.status, v.error

    seen = {}
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
    while seen.size < 3 && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      events = @client.poll_events(consumer, 100, 2000, 0)
      events.select { |e| e.instance_id == v.id }.each { |e| seen[e.type] = e }
      @client.ack_events(consumer, events.last.cursor) unless events.empty?
    end
    assert_empty %w[wf.started order.shipped wf.completed] - seen.keys, seen.keys.inspect
    assert_equal({ "attempt" => 1, "step" => "ship" }, seen["order.shipped"].payload)
    refute_nil seen["order.shipped"].node_id
    assert_equal [["ship", 1]], @client.step_stats(spec.name, nil, 0, 0).map { |n| [n.name, n.count] }
  end

  def test_queries
    spec = FlowSpec.define(unique("rb-it-query"), 1) { |f| f.then_apply(:a) }
    @client.register(spec)
    worker = Worker.new(@client, "it-query", OPTIONS).register_handler(spec.name, Class.new { def a(c) = c }.new).start
    begin
      id = @client.start(spec, { "k" => 1 }, nil, "biz-#{spec.name}")
      v = @client.await_completion(id, 30)
    ensure
      worker.close
    end
    assert_equal "COMPLETED", v.status, v.error
    assert_includes @client.workflow_names, spec.name
    assert_equal "a", @client.get_workflow(spec.name)["nodes"].find { |n| n["kind"] == "TASK" }["name"]
    assert_equal [id], @client.list_instances(spec.name, nil, 10).map(&:id)
    assert_equal [id], @client.find_by_correlation("biz-#{spec.name}").map(&:id)
    refute_empty @client.instance_detail(id).tokens
    assert_equal 404, assert_raises(WiggleApiException) { @client.get_workflow("#{spec.name}-missing") }.status
  end
end
