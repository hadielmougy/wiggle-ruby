# frozen_string_literal: true

require_relative "test_helper"

# Offline tests for Worker#register_handler and its start-time reconciliation against the registered
# graph -- no server required. A fake client stands in for the control plane.
class WorkerTest < Minitest::Test
  include Wiggle

  # Only the surface reconcile touches: get_workflow returning a canned graph (or a 404).
  class FakeClient
    attr_reader :requested

    def initialize(graphs)
      @graphs = graphs
      @requested = []
    end

    def get_workflow(name, version = nil)
      @requested << [name, version]
      @graphs.fetch(version ? "#{name}@#{version}" : name) { raise Wiggle::WiggleApiException.new(404, "no workflow #{name}") }
    end
  end

  module OrderSteps
    extend Wiggle::ForFlow

    def validate(o) = nil
    def in_stock?(o) = nil
    def authorise(o) = nil
    void def audit(o) = nil
  end

  class OrderHandlers
    extend Wiggle::ForFlow
    include OrderSteps
    for_flow "order-fulfilment"

    def validate(o) = o.merge("status" => "VALIDATED")
    def in_stock?(o) = o["qty"].positive?
    def authorise(o) = o.merge("paid" => true)
    def audit(o) = o.size
  end

  def order_graph
    FlowSpec.define("order-fulfilment", 1, OrderSteps) do |f, s|
      f.then_apply(s.validate).then_filter(s.in_stock?).then_apply(s.authorise, "payments").then_accept(s.audit)
    end.definition
  end

  def worker(graphs = { "order-fulfilment" => order_graph }, options = WorkerOptions.defaults)
    Worker.new(FakeClient.new(graphs), "w-test", options)
  end

  def test_register_handler_binds_by_name_and_learns_queues
    w = worker.register_handler(OrderHandlers.new)
    w.reconcile
    h = w.handlers
    assert_equal %w[order-fulfilment#audit order-fulfilment#authorise order-fulfilment#in_stock order-fulfilment#validate],
                 h.keys.sort
    assert_equal true, h["order-fulfilment#in_stock"].call({ "qty" => 2 }, nil)
    assert_equal({ "qty" => 1, "paid" => true }, h["order-fulfilment#authorise"].call({ "qty" => 1 }, nil))
    assert_nil h["order-fulfilment#audit"].call({ "orderId" => "o1" }, nil) # void: context unchanged
    assert_equal Set["order-fulfilment", "payments"], w.served_queues
  end

  def test_register_handler_with_an_explicit_workflow
    plain = Class.new { def validate(o) = o }.new
    w = worker.register_handler("order-fulfilment", plain)
    w.reconcile
    assert_equal ["order-fulfilment#validate"], w.handlers.keys
    assert_raises(ArgumentError) { worker.register_handler(plain) } # no for_flow, no name
  end

  def test_register_handler_for_one_version
    client = FakeClient.new({ "order-fulfilment@3" => order_graph })
    w = Worker.new(client, "w", WorkerOptions.defaults).register_handler(OrderHandlers.new, 3)
    w.reconcile
    assert_equal [["order-fulfilment", 3]], client.requested
    assert w.handlers.key?("order-fulfilment#validate@3")
    assert_equal [["order-fulfilment", 3]], w.send(:versions)
  end

  def test_an_unversioned_registration_claims_every_version
    w = worker({ "order-fulfilment" => order_graph, "order-fulfilment@3" => order_graph })
        .register_handler(OrderHandlers.new, 3)
        .register_handler(Class.new { def validate(o) = o }.new.tap { |h| h.define_singleton_method(:class) { OrderHandlers } })
    assert_equal [], w.send(:versions)
  end

  def test_explicit_queues_override_discovery
    w = worker({ "order-fulfilment" => order_graph }, WorkerOptions.defaults.with_queues("mine"))
    w.register_handler(OrderHandlers.new).reconcile
    assert_equal Set["mine"], w.served_queues
  end

  def test_a_missing_workflow_is_fatal
    w = worker({}).register_handler(OrderHandlers.new)
    assert_match(/is not registered/, assert_raises(ArgumentError) { w.reconcile }.message)
  end

  def test_await_registration_rides_out_a_late_register
    graphs = {}
    w = worker(graphs, WorkerOptions.defaults.with_await_registration(5)).register_handler(OrderHandlers.new)
    Thread.new do
      Kernel.sleep 0.3
      graphs["order-fulfilment"] = order_graph
    end
    w.reconcile
    assert_includes w.served_queues, "payments"
  end

  def test_duplicate_handlers_are_rejected
    w = worker.register_handler(OrderHandlers.new).register_handler(OrderHandlers.new)
    assert_match(/duplicate handler/, assert_raises(ArgumentError) { w.reconcile }.message)
  end

  def test_worker_options_are_immutable_builders
    o = WorkerOptions.defaults.with_concurrency(3).with_lease(5).with_long_poll_wait(1).with_queues("a", "b")
    assert_equal [3, 5, 1, %w[a b]], [o.concurrency, o.lease, o.long_poll_wait, o.queues]
    assert o.frozen?
    refute_equal 3, WorkerOptions.defaults.concurrency if Etc.nprocessors != 3
  end

  def test_latch_waits_end_early
    latch = Worker::Latch.new.set
    t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    Thread.new do
      Kernel.sleep 0.05
      latch.clear
    end
    assert latch.wait_cleared(5)
    assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0, :<, 1
    refute Worker::Latch.new.wait_set(0.01)
  end
end
