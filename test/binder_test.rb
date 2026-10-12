# frozen_string_literal: true

require_relative "test_helper"

# Wiggle::Binder in isolation -- the binder is pure (graph in -> bindings out), so every signature
# rule is testable with no server, no worker, no I/O. Mirrors the Java client's HandlerBinderTest.
class BinderTest < Minitest::Test
  include Wiggle

  def handlers(&blk) = Class.new(&blk).new

  # ---------------------------------------------------------------- scan

  def test_scan_rejects_case_fold_collisions
    h = handlers do
      def in_stock(c) = c
      def inStock(c) = c # rubocop:disable Naming/MethodName
    end
    assert_match(/ambiguous/, assert_raises(ArgumentError) { Binder.scan(h) }.message)
  end

  def test_scan_rejects_non_single_context_methods
    h = handlers { def work(c, extra) = c }
    assert_match(/single context argument/, assert_raises(ArgumentError) { Binder.scan(h) }.message)
  end

  def test_scan_rejects_an_object_with_no_handlers
    h = handlers { private def helper = 1 }
    assert_match(/no handler methods/, assert_raises(ArgumentError) { Binder.scan(h) }.message)
  end

  def test_scan_skips_private_helpers_setters_and_the_workflow_reader
    h = handlers do
      attr_writer :config

      def workflow = "wf"
      def work(c) = c

      private

      def helper(a, b, c) = nil
    end
    assert_equal ["work"], Binder.scan(h).keys
  end

  def test_scan_accepts_optional_and_splat_parameters
    h = handlers do
      def a(c, opt = 1) = c
      def b(*args) = args
      def c(ctx, flag: false) = ctx
    end
    assert_equal %w[a b c], Binder.scan(h).keys.sort
  end

  # ---------------------------------------------------------------- bind: kinds & wiring

  def linear = Graph.new("wf", [Step.new("work", queue: "special"), Gate.new("ok"), Step.new("other")]).compile.definition

  def test_bind_kinds_queues_and_unserved
    h = handlers do
      def work(c) = c.merge("done" => true)
      def ok?(c) = true
    end
    result = Binder.bind("wf", Binder.scan(h), linear)
    by_step = result.bindings.to_h { |b| [b.step, b] }
    assert_equal %w[ok work], by_step.keys.sort
    assert_equal "special", by_step["work"].queue # explicit queue respected
    assert_equal "wf", by_step["ok"].queue        # default queue = the workflow name
    assert_equal ["other"], result.unserved
    assert_equal({ "a" => 1, "done" => true }, by_step["work"].handler.call({ "a" => 1 }))
    assert_equal true, by_step["ok"].handler.call({})
  end

  def test_bind_effect_returns_nil
    h = Class.new(Wiggle::Handlers) do
      effect def work(c)
        c["seen"] = true
      end
    end.new
    handler = Binder.bind("wf", Binder.scan(h), linear).bindings.find { |b| b.step == "work" }.handler
    assert_nil handler.call({ "a" => 1 }) # context untouched on the wire
  end

  def test_bind_rejects_kind_mismatches
    assert_match(/did you mean a PREDICATE/,
                 assert_raises(ArgumentError) { Binder.bind("wf", Binder.scan(handlers { def work?(c) = true }), linear) }.message)
    effect_gate = Class.new(Wiggle::Handlers) { effect def ok(c) = c }.new
    assert_match(/must return a boolean/,
                 assert_raises(ArgumentError) { Binder.bind("wf", Binder.scan(effect_gate), linear) }.message)
  end

  def test_bind_rejects_a_handler_matching_no_step
    h = handlers { def nonexistent(c) = c }
    assert_match(/matches no step/, assert_raises(ArgumentError) { Binder.bind("wf", Binder.scan(h), linear) }.message)
  end

  # ---------------------------------------------------------------- combines

  def forked
    Graph.new("wf", [Fork.new([Branch.new("a", [Step.new("a1")]), Branch.new("b", [Step.new("b1")])], combine: "merge")])
         .compile.definition
  end

  def test_fork_combine_returns_verbatim
    h = handlers do
      def a1(c) = c
      def b1(c) = c
      def merge(c) = { "pre" => c["pre"] }.merge(c["a"], c["b"])
    end
    merge = Binder.bind("wf", Binder.scan(h), forked).bindings.find { |b| b.step == "merge" }.handler
    assert_equal({ "pre" => "P", "x" => 1, "y" => 2 }, merge.call({ "pre" => "P", "a" => { "x" => 1 }, "b" => { "y" => 2 } }))
  end

  def test_combine_cannot_be_an_effect
    h = Class.new(Wiggle::Handlers) do
      def a1(c) = c
      def b1(c) = c
      effect def merge(c) = nil
    end.new
    assert_match(/complete post-join context/, assert_raises(ArgumentError) { Binder.bind("wf", Binder.scan(h), forked) }.message)
  end

  def test_legacy_items_key_is_still_a_combine
    assert Binder.combine?({ "kind" => "TASK", "itemsKey" => '["a","b"]' })
    assert Binder.combine?({ "kind" => "TASK", "armNames" => %w[a b] })
    assert Binder.combine?({ "kind" => "TASK", "collectKey" => "__forEach__x" })
    refute Binder.combine?({ "kind" => "TASK", "armNames" => [] })
    refute Binder.combine?({ "kind" => "PREDICATE", "armNames" => ["a"] })
  end

  def test_unstage_presents_fork_arms_by_name
    ctx, base = Binder.unstage({ "n" => 1, "__arm__l" => { "s" => "L" }, "__arm__r" => { "s" => "R" } }, %w[l r], "", "merge")
    assert_equal({ "n" => 1, "l" => { "s" => "L" }, "r" => { "s" => "R" } }, ctx)
    assert_equal({ "n" => 1 }, base)
  end

  def test_unstage_presents_for_each_results_by_name
    ctx, base = Binder.unstage({ "items" => [1, 2], "__forEach__per-item" => %w[a b],
                                 "__steps____forEach__per-item" => %w[norm norm] }, [], "__forEach__per-item", "collect")
    assert_equal({ "items" => [1, 2], "per-item" => %w[a b] }, ctx)
    assert_equal({ "items" => [1, 2] }, base)
  end

  def test_unstage_refuses_a_name_the_context_already_has
    err = assert_raises(ArgumentError) { Binder.unstage({ "payment" => 1, "__arm__payment" => {} }, ["payment"], "", "merge") }
    assert_match(/'payment' is both/, err.message)
  end

  def test_unstage_leaves_other_steps_alone
    assert_equal [{ "__arm__x" => 1 }, nil], Binder.unstage({ "__arm__x" => 1 }, [], "", "s")
  end

  # ---------------------------------------------------------------- compensation pairing

  def saga = Graph.new("saga", [Step.new("reserve", compensate: true), Step.new("charge", queue: "payments")]).compile.definition

  def test_compensator_binds_and_splits_snapshots
    seen = []
    h = handlers do
      define_method(:reserve) { |c| c }
      define_method(:charge) { |c| c }
      define_method(:compensate_reserve) { |comp| seen << comp }
    end
    comp = Binder.bind("saga", Binder.scan(h), saga).bindings.find { |b| b.activity == "saga#reserve#compensate" }
    assert_equal "saga", comp.queue, "compensator inherits the forward step's queue"
    out = comp.handler.call({ "input" => { "orderId" => "o1" }, "result" => { "orderId" => "o1", "reservationRef" => "r-9" } })
    assert_nil out, "an undo never changes the instance context"
    assert_equal({ "orderId" => "o1" }, seen.first.input)
    assert_equal({ "orderId" => "o1", "reservationRef" => "r-9" }, seen.first.result)
  end

  def test_compensable_step_without_compensator_refuses_to_bind
    h = handlers do
      def reserve(c) = c
      def charge(c) = c
    end
    assert_match(/compensate_reserve/, assert_raises(ArgumentError) { Binder.bind("saga", Binder.scan(h), saga) }.message)
  end

  def test_compensator_on_non_compensable_step_refuses_to_bind
    h = handlers do
      def reserve(c) = c
      def charge(c) = c
      def compensate_reserve(comp) = nil
      def compensate_charge(comp) = nil # charge is NOT compensable
    end
    assert_match(/compensate_charge/, assert_raises(ArgumentError) { Binder.bind("saga", Binder.scan(h), saga) }.message)
  end

  def test_step_literally_named_compensate_x_stays_a_forward_handler
    graph = Graph.new("wf", [Step.new("compensate-order")]).compile.definition
    h = handlers { def compensate_order(c) = c.merge("handled" => true) }
    assert_equal ["wf#compensate-order"], Binder.bind("wf", Binder.scan(h), graph).bindings.map(&:activity)
  end
end
