# frozen_string_literal: true

require_relative "test_helper"

# Offline tests for name-only handler binding (Worker#handle and friends) and its start-time
# reconciliation against the server's registered graph -- no server required. A tiny fake client
# stands in for the control plane, returning a canned graph from #get_workflow.
class WorkerTest < Minitest::Test
  include Wiggle

  # Only the surface reconcile touches: get_workflow returning a canned Hash (or NOT_FOUND).
  class FakeClient
    def initialize(graphs) = @graphs = graphs

    def get_workflow(name, version: nil) = @graphs.fetch(name) { raise GRPC::NotFound, name }
  end

  # The registered graph as get_workflow returns it: a (say, Java-authored) order flow that a Ruby
  # worker implements steps of, by name.
  def order_graph(authorise_queue: "payments")
    Graph.new("order-fulfilment", [Step.new("validate"), Gate.new("in-stock"),
                                   Step.new("authorise", queue: authorise_queue), Effect.new("audit")]).compile.definition
  end

  def worker(graphs = { "order-fulfilment" => order_graph }, **kw) = Worker.new(FakeClient.new(graphs), "w-test", **kw)

  # ---------------------------------------------------------------- binding + reconcile

  def test_handle_registers_by_activity_name
    w = worker.handle("order-fulfilment", "authorise") { |o| o.merge("paid" => true) }
    assert w.handlers.key?("order-fulfilment#authorise")
  end

  def test_handle_accepts_any_callable
    w = worker.handle("order-fulfilment", "authorise", ->(o) { o.merge("paid" => true) })
    assert_equal({ "paid" => true }, w.handlers["order-fulfilment#authorise"].call({}))
    assert_raises(ArgumentError) { worker.handle("order-fulfilment", "authorise") }
  end

  def test_reconcile_discovers_the_steps_queue
    w = worker.handle("order-fulfilment", "authorise") { |o| o }
    w.reconcile
    # the worker had no topology, so the only queue it now polls is the one learned from the graph
    assert_equal Set["payments"], w.served_queues
  end

  def test_reconcile_defaults_queue_to_workflow_name_when_unset
    graph = order_graph
    graph["nodes"].find { |n| n["name"] == "authorise" }.delete("queue")
    w = worker({ "order-fulfilment" => graph }).handle("order-fulfilment", "authorise") { |o| o }
    w.reconcile
    assert_equal Set["order-fulfilment"], w.served_queues
  end

  def test_explicit_queues_override_discovery
    w = worker(queues: ["mine"]).handle("order-fulfilment", "authorise") { |o| o }
    w.reconcile
    assert_equal Set["mine"], w.served_queues
  end

  def test_reconcile_rejects_unknown_step
    w = worker.handle("order-fulfilment", "autorise") { |o| o } # typo
    err = assert_raises(ArgumentError) { w.reconcile }
    assert_match(/no step 'autorise'.*available steps/, err.message)
  end

  def test_reconcile_rejects_kind_mismatch
    w = worker.handle("order-fulfilment", "in-stock") { |o| o } # a PREDICATE bound as a task
    err = assert_raises(ArgumentError) { w.reconcile }
    assert_match(/is a PREDICATE.*handle_gate/, err.message)
  end

  def test_handle_gate_matches_a_predicate_node
    worker.handle_gate("order-fulfilment", "in-stock") { |o| o["qty"].positive? }.reconcile
  end

  def test_reconcile_missing_workflow_is_fatal
    w = worker({}).handle("order-fulfilment", "authorise") { |o| o }
    err = assert_raises(ArgumentError) { w.reconcile }
    assert_match(/is not registered/, err.message)
  end

  def test_await_registration_rides_out_a_late_register
    graphs = {}
    w = worker(graphs, await_registration: 5).handle("order-fulfilment", "authorise") { |o| o }
    Thread.new do
      Kernel.sleep 0.3
      graphs["order-fulfilment"] = order_graph
    end
    w.reconcile
    assert_equal Set["payments"], w.served_queues
  end

  def test_duplicate_binding_rejected
    w = worker.handle("order-fulfilment", "authorise") { |o| o }
    err = assert_raises(ArgumentError) { w.handle("order-fulfilment", "authorise") { |o| o } }
    assert_match(/duplicate handler/, err.message)
  end

  # ---------------------------------------------------------------- wrapper semantics

  def test_handle_wrapper_sends_the_whole_return
    w = worker.handle("order-fulfilment", "authorise") { |o| o.merge("paid" => true) }
    assert_equal({ "orderId" => "o1", "paid" => true }, w.handlers["order-fulfilment#authorise"].call({ "orderId" => "o1" }))
  end

  def test_handle_effect_wrapper_returns_nil
    seen = {}
    w = worker.handle_effect("order-fulfilment", "audit") { |o| seen.update(o) }
    assert_nil w.handlers["order-fulfilment#audit"].call({ "orderId" => "o1" })
    assert_equal({ "orderId" => "o1" }, seen)
  end

  def test_handle_gate_wrapper_returns_a_boolean
    w = worker.handle_gate("order-fulfilment", "in-stock") { |o| o["qty"] if o["qty"].positive? }
    assert_equal true, w.handlers["order-fulfilment#in-stock"].call({ "qty" => 3 })
    assert_equal false, w.handlers["order-fulfilment#in-stock"].call({ "qty" => 0 })
  end

  # ---------------------------------------------------------------- combines

  def forked_graph
    Graph.new("wf", [Fork.new([Branch.new("a", [Step.new("a1")]), Branch.new("b", [Step.new("b1")])], combine: "merge")])
         .compile.definition
  end

  def test_handle_combine_must_target_a_combine
    w = worker({ "wf" => forked_graph }).handle_combine("wf", "a1") { |c| c }
    assert_match(/not a fork combine/, assert_raises(ArgumentError) { w.reconcile }.message)
    w = worker({ "wf" => forked_graph }).handle("wf", "merge") { |c| c }
    assert_match(/handle_combine/, assert_raises(ArgumentError) { w.reconcile }.message)
    worker({ "wf" => forked_graph }).handle_combine("wf", "merge") { |c| c }.reconcile
  end

  # ---------------------------------------------------------------- register_handlers (object)

  def test_canonical_folds_case_styles
    ["in-stock", "in_stock", "inStock", "InStock", "instock", "IN_STOCK", "in stock", "in_stock?"].each do |name|
      assert_equal "instock", Binder.canonical(name), name
    end
    assert_equal "autoapprove", Binder.canonical("autoApprove")
    assert_equal "httpserver", Binder.canonical("HTTPServer")
  end

  class OrderHandlers < Wiggle::Handlers
    workflow "order-fulfilment"

    def validate(o) = o.merge("status" => "VALIDATED")

    def in_stock?(o) = o["qty"].positive? # predicate-style name -> matches the graph's "in-stock" gate

    def authorise(o) = o.merge("paid" => true)

    effect def audit(o) # a side effect: context unchanged
      o["audited"] = true
    end

    private

    def helper(a, b) = [a, b] # private: a helper, not a step handler
  end

  def test_register_handlers_matches_by_name_and_binds_all_kinds
    w = worker.register_handlers(OrderHandlers.new) # workflow taken from the class
    w.reconcile
    h = w.handlers
    assert_equal %w[order-fulfilment#audit order-fulfilment#authorise order-fulfilment#in-stock order-fulfilment#validate],
                 h.keys.sort
    assert_equal true, h["order-fulfilment#in-stock"].call({ "qty" => 2 })
    assert_equal false, h["order-fulfilment#in-stock"].call({ "qty" => 0 })
    assert_equal({ "qty" => 1, "paid" => true }, h["order-fulfilment#authorise"].call({ "qty" => 1 }))
    assert_nil h["order-fulfilment#audit"].call({ "orderId" => "o1" })
    assert_includes w.served_queues, "payments"
  end

  def test_register_handlers_takes_an_explicit_workflow_and_plain_objects
    plain = Class.new do
      def validate(o) = o
      def audit(o) = o.size
    end
    w = worker.register_handlers(plain.new, workflow: "order-fulfilment", effects: [:audit])
    w.reconcile
    assert_nil w.handlers["order-fulfilment#audit"].call({ "a" => 1 })
    assert_raises(ArgumentError) { worker.register_handlers(plain.new, workflow: "order-fulfilment", effects: [:nope]) }
  end

  def test_register_handlers_needs_a_workflow_name
    h = Class.new(Wiggle::Handlers) { def validate(o) = o }
    assert_match(/needs a workflow name/, assert_raises(ArgumentError) { worker.register_handlers(h.new) }.message)
  end

  def test_register_handlers_rejects_case_fold_duplicates
    h = Class.new(Wiggle::Handlers) do
      workflow "order-fulfilment"
      def in_stock(o) = true
      def inStock(o) = true # rubocop:disable Naming/MethodName -- the collision is the point
    end
    err = assert_raises(ArgumentError) { worker.register_handlers(h.new) }
    assert_match(/both map to the same step name 'instock'/, err.message)
  end

  def test_register_handlers_rejects_method_matching_no_step
    h = Class.new(Wiggle::Handlers) do
      workflow "order-fulfilment"
      def validate(o) = o
      def ship_it_now(o) = o
    end
    w = worker.register_handlers(h.new)
    assert_match(/handler 'ship_it_now' matches no step/, assert_raises(ArgumentError) { w.reconcile }.message)
  end

  def test_register_handlers_rejects_effect_on_a_gate
    h = Class.new(Wiggle::Handlers) do
      workflow "order-fulfilment"
      effect def in_stock(o) = o
    end
    w = worker.register_handlers(h.new)
    assert_match(/is a gate .* marked as an effect/, assert_raises(ArgumentError) { w.reconcile }.message)
  end

  def test_register_handlers_rejects_a_predicate_name_on_a_task
    h = Class.new(Wiggle::Handlers) do
      workflow "order-fulfilment"
      def validate?(o) = true
    end
    w = worker.register_handlers(h.new)
    assert_match(/did you mean a PREDICATE/, assert_raises(ArgumentError) { w.reconcile }.message)
  end

  def test_register_handlers_rejects_bad_arity
    h = Class.new(Wiggle::Handlers) do
      workflow "order-fulfilment"
      def validate(o, extra) = o
    end
    err = assert_raises(ArgumentError) { worker.register_handlers(h.new) }
    assert_match(/must accept a single context argument/, err.message)
  end

  def test_register_handlers_and_handle_conflict_is_a_duplicate
    w = worker.handle("order-fulfilment", "validate") { |o| o }.register_handlers(OrderHandlers.new)
    err = assert_raises(ArgumentError) { w.reconcile }
    assert_match(/duplicate handler for activity 'order-fulfilment#validate'/, err.message)
  end

  def test_handlers_subclass_inherits_workflow_and_effects
    sub = Class.new(OrderHandlers)
    assert_equal "order-fulfilment", sub.workflow
    assert_includes sub.effects, "audit"
  end

  # ---------------------------------------------------------------- compensation (explicit surface)

  def saga_graph = Graph.new("saga", [Step.new("reserve", compensate: true), Step.new("boom")]).compile.definition

  def test_handle_compensation_registers_and_reconciles
    seen = nil
    w = worker({ "saga" => saga_graph })
        .handle("saga", "reserve") { |o| o }
        .handle("saga", "boom") { |o| o }
        .handle_compensation("saga", "reserve") { |comp| seen = comp }
    w.reconcile
    assert_nil w.handlers["saga#reserve#compensate"].call({ "input" => { "a" => 1 }, "result" => { "b" => 2 } })
    assert_equal({ "a" => 1 }, seen.input)
    assert_equal({ "b" => 2 }, seen.result)
  end

  def test_forward_handler_on_compensable_step_requires_the_compensator
    w = worker({ "saga" => saga_graph }).handle("saga", "reserve") { |o| o }
    assert_match(/handle_compensation/, assert_raises(ArgumentError) { w.reconcile }.message)
  end

  def test_handle_compensation_on_non_compensable_step_is_rejected
    w = worker({ "saga" => saga_graph }).handle_compensation("saga", "boom") { |_| nil }
    assert_match(/not declared compensate: true/, assert_raises(ArgumentError) { w.reconcile }.message)
  end

  # ---------------------------------------------------------------- latch

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
