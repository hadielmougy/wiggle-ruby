# frozen_string_literal: true

require_relative "test_helper"

# Wiggle::HandlerBinder in isolation -- the binder is pure (graph in -> bindings out), so every
# shape rule is testable with no server, no worker, no I/O. Mirrors the Java HandlerBinderTest.
class BinderTest < Minitest::Test
  include Wiggle

  def handlers(&blk) = Class.new(&blk).new

  def bind(target, graph, workflow = "wf") = HandlerBinder.bind(HandlerBinder.scan(workflow, target), graph)

  def by_step(result) = result.bindings.to_h { |b| [b.activity, b] }

  def linear
    FlowSpec.define("wf", 1) { |f| f.then_apply(:work, "special").then_filter(:ok?).then_apply(:other) }.definition
  end

  # ---------------------------------------------------------------- scan

  def test_canonical_folds_case_styles
    ["in-stock", "in_stock", "inStock", "InStock", "instock", "IN_STOCK", "in stock", "in_stock?"].each do |n|
      assert_equal "instock", HandlerBinder.canonical(n), n
    end
    assert_equal "httpserver", HandlerBinder.canonical("HTTPServer")
  end

  def test_scan_rejects_case_fold_collisions
    h = handlers do
      def in_stock(c) = c
      def inStock(c) = c # rubocop:disable Naming/MethodName -- the collision is the point
    end
    assert_match(/ambiguous/, assert_raises(ArgumentError) { HandlerBinder.scan("wf", h) }.message)
  end

  def test_scan_skips_private_helpers_setters_and_keyword_only_methods
    h = handlers do
      attr_writer :config

      def work(c) = c
      def configure(level:) = level

      private

      def helper(a, b, c) = nil
    end
    assert_equal ["work"], HandlerBinder.scan("wf", h).by_name.keys
  end

  def test_scan_rejects_an_object_with_no_handlers
    assert_raises(ArgumentError) { HandlerBinder.scan("wf", handlers { private def helper = 1 }) }
  end

  # ---------------------------------------------------------------- kinds & wiring

  def test_bind_kinds_queues_and_unserved
    h = handlers do
      def work(c) = c.merge("done" => true)
      def ok?(c) = c["n"]
    end
    result = bind(h, linear)
    b = by_step(result)
    assert_equal "special", b["wf#work"].queue # explicit queue respected
    assert_equal "wf", b["wf#ok"].queue        # default queue = the workflow name
    assert_equal ["other"], result.unserved
    assert_equal({ "a" => 1, "done" => true }, b["wf#work"].handler.call({ "a" => 1 }, nil))
    assert_equal true, b["wf#ok"].handler.call({ "n" => 1 }, nil) # truthiness becomes a boolean
    assert_equal false, b["wf#ok"].handler.call({}, nil)
  end

  def test_void_methods_are_effects
    h = Class.new do
      extend Wiggle::ForFlow
      void def work(c) = c.merge("seen" => true)
    end.new
    assert_nil by_step(bind(h, linear))["wf#work"].handler.call({ "a" => 1 }, nil)
  end

  def test_void_declared_on_an_included_contract
    contract = Module.new do
      extend Wiggle::ForFlow
      void def work(c) = nil
    end
    h = Class.new do
      include contract
      def work(c) = true # a stray return value must not replace the context
    end.new
    assert_nil by_step(bind(h, linear))["wf#work"].handler.call({}, nil)
  end

  def test_shape_mismatches_are_rejected
    assert_match(/did you mean then_filter/,
                 assert_raises(ArgumentError) { bind(handlers { def work?(c) = true }, linear) }.message)
    void_gate = Class.new do
      extend Wiggle::ForFlow
      void def ok(c) = nil
    end.new
    assert_match(/gate .* is void/, assert_raises(ArgumentError) { bind(void_gate, linear) }.message)
    assert_match(/must take 1 argument/, assert_raises(ArgumentError) { bind(handlers { def work(a, b) = a }, linear) }.message)
    assert_match(/matches no step/, assert_raises(ArgumentError) { bind(handlers { def nope(c) = c }, linear) }.message)
  end

  def test_handles_renames_the_step_a_method_serves
    h = Class.new do
      extend Wiggle::ForFlow
      handles "work", def do_work(c) = c.merge("x" => 1)
    end.new
    assert_equal({ "x" => 1 }, by_step(bind(h, linear))["wf#work"].handler.call({}, nil))
  end

  # ---------------------------------------------------------------- activity factories

  class Doubler
    include Wiggle::Activity
    def execute(c) = c * 2
  end

  class Positive
    include Wiggle::GateActivity
    def test(c) = c.positive?
  end

  class Audit
    include Wiggle::EffectActivity
    attr_reader :seen

    def apply(c) = (@seen = c)
  end

  def test_factories_serve_tasks_gates_and_effects
    audit = Audit.new
    h = handlers do
      define_method(:work) { Doubler.new }
      define_method(:ok) { Positive.new }
      define_method(:other) { audit }
    end
    b = by_step(bind(h, linear))
    assert_equal 6, b["wf#work"].handler.call(3, nil)
    assert_equal true, b["wf#ok"].handler.call(3, nil)
    assert_nil b["wf#other"].handler.call(9, nil)
    assert_equal 9, audit.seen
    assert_raises(ArgumentError) { bind(handlers { define_method(:ok) { Doubler.new } }, linear) }
    assert_raises(ArgumentError) { bind(handlers { define_method(:work) { Object.new } }, linear) }
  end

  # ---------------------------------------------------------------- compensation

  class Reserve
    include Wiggle::CompensableActivity
    attr_reader :undone

    def execute(c) = c.merge("ref" => "r-1")
    def compensate(comp) = (@undone = comp)
  end

  def saga = FlowSpec.define("saga", 1) { |f| f.then_apply_compensable(:reserve).then_apply(:charge, "payments") }.definition

  def test_compensable_activity_binds_forward_and_undo
    reserve = Reserve.new
    h = handlers do
      define_method(:reserve) { reserve }
      define_method(:charge) { |c| c }
    end
    b = by_step(bind(h, saga, "saga"))
    assert_equal({ "ref" => "r-1" }, b["saga#reserve"].handler.call({}, nil))
    comp = b["saga#reserve#compensate"]
    assert_equal "saga", comp.queue, "the undo is minted on the forward step's queue"
    assert_nil comp.handler.call({ "input" => { "orderId" => "o1" }, "result" => { "ref" => "r-1" } }, nil)
    assert_equal [{ "orderId" => "o1" }, { "ref" => "r-1" }], [reserve.undone.input, reserve.undone.result]
  end

  def test_compensable_step_needs_a_compensable_activity
    h = handlers do
      def reserve(c) = c
      def charge(c) = c
    end
    assert_match(/CompensableActivity/, assert_raises(ArgumentError) { bind(h, saga, "saga") }.message)
  end

  def test_compensable_activity_on_a_plain_step_is_refused
    h = handlers do
      define_method(:reserve) { Reserve.new }
      define_method(:charge) { Reserve.new } # charge is NOT compensable: its undo would never run
    end
    assert_match(/not declared compensable/, assert_raises(ArgumentError) { bind(h, saga, "saga") }.message)
  end

  # ---------------------------------------------------------------- combines

  def forked
    FlowSpec.define("wf", 1) do |f|
      Wiggle.all_of(f.then_apply(:charge), f.then_apply(:reserve)).combine(:settle)
    end.definition
  end

  def test_fork_combine_takes_arms_in_order_and_an_optional_base
    with_base = handlers do
      def charge(c) = c
      def reserve(c) = c
      def settle(payment, shipping, base) = base.merge(payment).merge(shipping)
    end
    settle = by_step(bind(with_base, forked))["wf#settle"].handler
    assert_equal({ "n" => 1, "p" => 1, "s" => 2 }, settle.call([{ "p" => 1 }, { "s" => 2 }], { "n" => 1 }))

    arms_only = handlers do
      def charge(c) = c
      def reserve(c) = c
      def settle(payment, shipping) = payment.merge(shipping)
    end
    assert_equal({ "p" => 1, "s" => 2 }, by_step(bind(arms_only, forked))["wf#settle"].handler.call([{ "p" => 1 }, { "s" => 2 }], {}))
  end

  def test_combine_shape_is_checked
    wrong = handlers do
      def charge(c) = c
      def reserve(c) = c
      def settle(only) = only
    end
    assert_match(/must take 2 argument/, assert_raises(ArgumentError) { bind(wrong, forked) }.message)
  end

  def test_for_each_combine_takes_the_collected_results
    graph = FlowSpec.define("wf", 1) { |f| f.then_for_each("items") { |i| i.then_apply(:price) }.combine(:total) }.definition
    h = handlers do
      def price(i) = i
      def total(prices, base) = base.merge("total" => prices.sum)
    end
    total = by_step(bind(h, graph))["wf#total"].handler
    assert_equal({ "k" => 1, "total" => 6 }, total.call([[1, 2, 3]], { "k" => 1 }))
  end

  def test_unstage_splits_staged_results_from_the_base
    inputs, base = HandlerBinder.unstage({ "n" => 1, "__arm__l" => { "s" => "L" }, "__arm__r" => { "s" => "R" } }, %w[l r], "")
    assert_equal [[{ "s" => "L" }, { "s" => "R" }], { "n" => 1 }], [inputs, base]
    # a forEach named after its collection key: the results never collide with the collection itself
    inputs, base = HandlerBinder.unstage({ "items" => [1], "__forEach__items" => ["a"], "__steps____forEach__items" => ["p"] },
                                         [], "__forEach__items")
    assert_equal [[["a"]], { "items" => [1] }], [inputs, base]
    assert_equal [nil, nil], HandlerBinder.unstage({ "__arm__x" => 1 }, [], "")
  end

  def test_combine_nodes_are_recognised
    assert HandlerBinder.combine?({ "kind" => "TASK", "armNames" => %w[a b] })
    assert HandlerBinder.combine?({ "kind" => "TASK", "collectKey" => "__forEach__x" })
    refute HandlerBinder.combine?({ "kind" => "TASK", "armNames" => [] })
    refute HandlerBinder.combine?({ "kind" => "PREDICATE", "armNames" => ["a"] })
  end
end
