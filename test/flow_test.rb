# frozen_string_literal: true

require_relative "test_helper"

# Offline tests for FlowSpec.define and the WiggleFlow chain -- no server required. They define
# workflows and assert on the compiled topology: node kinds, edge wiring, queues, retry, and the
# definition-time checks (an uncombined fan-out, a malformed choice, ...).
class FlowTest < Minitest::Test
  include Wiggle
  include TestSupport

  module Steps
    extend Wiggle::ForFlow

    def validate(o) = nil
    def in_stock?(o) = nil
    def charge(o) = nil
    def reserve(o) = nil
    def label(o) = nil
    def settle(a, b) = nil
    def notify(o) = nil
    def vip?(o) = nil
    def big?(o) = nil
    def concierge(o) = nil
    def thanks(o) = nil
    def standard(o) = nil
    def more?(o) = nil
    def fetch(o) = nil
    def price(item) = nil
    def collect(items) = nil
    def auto_approve(o) = nil
    def authorise = nil
    handles "capture-payment", def capture(o) = nil
  end

  def define(name = "wf", version = 1, *opts, &blk) = FlowSpec.define(name, version, *opts, Steps, &blk)

  # ---------------------------------------------------------------- linear + gate

  def test_linear_chain_and_gated_end
    spec = define { |f, s| f.then_apply(s.validate).then_filter(s.in_stock?).then_accept(s.notify) }
    a, g, b = named(spec, "validate"), named(spec, "in_stock"), named(spec, "notify")
    assert_equal a["id"], spec.definition["startNode"]
    assert_equal %w[TASK PREDICATE TASK], [a, g, b].map { |n| n["kind"] }
    assert_equal g["id"], a["next"]
    assert_equal b["id"], g["next"]
    assert_equal "wf#validate", a["activity"]
    gated = by_id(spec)[g["altNext"]] # a failed filter ends the instance as gated:<name>
    assert_equal ["END", "gated:in_stock"], [gated["kind"], gated["reason"]]
    assert_equal 1, spec.version
    assert_equal "wf", spec.name
  end

  def test_names_come_from_method_references
    handlers = Class.new do
      def ship(o) = o
    end.new
    spec = FlowSpec.define("wf", 1) { |f| f.then_apply(handlers.method(:ship)).then_apply(:wrap!) }
    assert_equal %w[ship wrap], spec.definition["nodes"].filter_map { |n| n["name"] if n["kind"] == "TASK" }
    assert_equal "capture-payment", named(define { |f, s| f.then_apply(s.capture) }, "capture-payment")["name"]
  end

  def test_blocks_and_strings_are_not_step_references
    assert_raises(ArgumentError) { FlowSpec.define("wf", 1) { |f| f.then_apply(->(o) { o }) } }
    assert_raises(ArgumentError) { FlowSpec.define("wf", 1) { |f| f.then_apply("validate") } }
    assert_raises(NoMethodError) { define { |f, s| f.then_apply(s.not_declared) } }
  end

  def test_retry_and_queue_in_either_order
    spec = define do |f, s|
      f.then_apply(s.validate, "payments", RetryPolicy.exponential(5, 0.1))
       .then_apply(s.charge, RetryPolicy.none, "cards")
    end
    assert_equal({ "maxAttempts" => 5, "initialBackoffMillis" => 100, "multiplier" => 2.0,
                   "maxBackoffMillis" => 300_000, "jitter" => 0.2 }, named(spec, "validate")["retry"])
    assert_equal "payments", named(spec, "validate")["queue"]
    assert_equal ["cards", 1], [named(spec, "charge")["queue"], named(spec, "charge")["retry"]["maxAttempts"]]
    assert_equal %w[cards payments], spec.queues
    assert_raises(ArgumentError) { define { |f, s| f.then_apply(s.validate, 3) } }
  end

  def test_default_retry_and_default_queue
    spec = define("wf", 1, RetryPolicy.fixed(3, 2)) do |f, s|
      f.then_apply(s.validate).default_queue("shared").then_apply(s.charge)
    end
    assert_equal({ "maxAttempts" => 3, "initialBackoffMillis" => 2000, "multiplier" => 1.0,
                   "maxBackoffMillis" => 2000, "jitter" => 0.0 }, named(spec, "validate")["retry"])
    assert_equal "wf", named(spec, "validate")["queue"]
    assert_equal "shared", named(spec, "charge")["queue"]
    assert_equal 10_000, named(define { |f, s| f.then_apply(s.validate) }, "validate")["retry"]["maxAttempts"]
  end

  def test_version_is_declared_and_validated
    assert_equal 42, define("wf", 42) { |f, s| f.then_apply(s.validate) }.definition["version"]
    assert_raises(ArgumentError) { define("wf", 0) { |f, s| f.then_apply(s.validate) } }
    assert_raises(TypeError) { define("wf", "1") { |f, s| f.then_apply(s.validate) } }
  end

  def test_body_must_return_a_handle_and_record_a_step
    assert_raises(ArgumentError) { define { |_f, _s| nil } }
    assert_raises(ArgumentError) { define { |f, _s| f } }
    assert_raises(ArgumentError) { FlowSpec.define("wf", 1) }
  end

  def test_duplicate_step_rejected
    assert_raises(ArgumentError) { define { |f, s| f.then_apply(s.validate).then_apply(s.validate) } }
  end

  # ---------------------------------------------------------------- timers / signals / sub-flows

  def test_sleep_await_and_sub_flow
    spec = define do |f, s|
      f.then_sleep(0.25).then_sleep("nap", 1).then_await("go", 2).then_sub_flow("call", "child").then_apply(s.validate)
    end
    assert_equal ["SLEEP", 250], named(spec, "sleep-250ms").values_at("kind", "sleepMillis")
    assert_equal 1000, named(spec, "nap")["sleepMillis"]
    assert_equal ["SIGNAL", 2000], named(spec, "go").values_at("kind", "sleepMillis")
    assert_equal %w[SUB_WORKFLOW child], named(spec, "call").values_at("kind", "activity")
    refute named(spec, "go").key?("altNext")
  end

  def test_await_escalation_branch_rejoins
    spec = define do |f, s|
      f.then_apply(s.validate).then_await("approval", 60) { |esc| esc.then_apply(s.auto_approve) }.then_apply(s.charge)
    end
    sig, auto, charge = named(spec, "approval"), named(spec, "auto_approve"), named(spec, "charge")
    assert_equal charge["id"], sig["next"]    # delivery continues
    assert_equal auto["id"], sig["altNext"]   # timeout escalates
    assert_equal charge["id"], auto["next"]   # escalation rejoins
    assert_raises(ArgumentError) { define { |f, s| f.then_await("x") { |e| e.then_apply(s.validate) } } }
  end

  # ---------------------------------------------------------------- all_of / combine

  def test_all_of_forks_from_the_shared_handle_and_names_arms_after_their_last_step
    spec = define do |f, s|
      validated = f.then_apply(s.validate)
      payment = validated.then_apply(s.charge)
      shipping = validated.then_apply(s.reserve).then_apply(s.label)
      Wiggle.all_of(payment, shipping).combine(s.settle, "merges").then_accept(s.notify)
    end
    fork, join = of_kind(spec, "FORK").first, of_kind(spec, "JOIN").first
    assert_equal fork["id"], named(spec, "validate")["next"] # the fork lands where the handle was continued twice
    assert_equal [named(spec, "charge")["id"], named(spec, "reserve")["id"]], fork["branches"]
    assert_equal 2, join["expected"]
    settle = named(spec, "settle")
    assert_equal ["TASK", %w[charge label], "merges"], settle.values_at("kind", "armNames", "queue")
    assert_equal settle["id"], join["next"]
    assert_equal named(spec, "notify")["id"], settle["next"]
    assert_equal join["id"], named(spec, "label")["next"]
  end

  def test_a_handle_continued_twice_must_be_combined
    err = assert_raises(ArgumentError) do
      define do |f, s|
        v = f.then_apply(s.validate)
        v.then_apply(s.charge)
        v.then_apply(s.reserve)
      end
    end
    assert_match(/after 'validate' splits into \['charge', 'reserve'\] but is never rejoined/, err.message)
  end

  def test_all_of_needs_a_combine_and_two_sibling_arms
    assert_match(/has no merge/, assert_raises(ArgumentError) do
      define { |f, s| Wiggle.all_of(f.then_apply(s.charge), f.then_apply(s.reserve)) && f }
    end.message)
    assert_raises(ArgumentError) { define { |f, s| Wiggle.all_of(f.then_apply(s.charge)).combine(s.settle) } }
    assert_raises(ArgumentError) do
      define do |f, s|
        a = f.then_apply(s.charge)
        Wiggle.all_of(a, a.then_apply(s.reserve)).combine(s.settle)
      end
    end
    assert_raises(ArgumentError) do
      define do |f, s|
        fork = Wiggle.all_of(f.then_apply(s.charge), f.then_apply(s.reserve))
        fork.combine(s.settle)
        fork.combine(s.label)
      end
    end
  end

  def test_filter_inside_an_arm_short_circuits_to_the_join
    spec = define do |f, s|
      Wiggle.all_of(f.then_filter(s.in_stock?).then_apply(s.charge), f.then_apply(s.reserve)).combine(s.settle)
    end
    assert_equal of_kind(spec, "JOIN").first["id"], named(spec, "in_stock")["altNext"]
  end

  def test_combine_by_name
    spec = define { |f, s| Wiggle.all_of(f.then_apply(s.charge), f.then_apply(s.reserve)).combine("merge-it") }
    assert_equal "TASK", named(spec, "merge-it")["kind"]
  end

  # ---------------------------------------------------------------- then_for_each

  def test_for_each_dynfork_join_and_mandatory_combine
    spec = define do |f, s|
      f.then_for_each("items") { |item| item.then_apply(s.price) }.combine(s.collect).then_apply(s.validate)
    end
    df, join = of_kind(spec, "DYN_FORK").first, of_kind(spec, "JOIN").first
    assert_equal %w[items items], df.values_at("name", "itemsKey")
    assert_equal [named(spec, "price")["id"]], df["branches"]
    assert_equal join["id"], df["next"] # an empty collection skips to the join
    refute join.key?("expected")
    collect = named(spec, "collect")
    assert_equal "__forEach__items", collect["collectKey"]
    assert_equal collect["id"], join["next"]
    assert_equal named(spec, "validate")["id"], collect["next"]
    assert_equal "prices", of_kind(define { |f, s| f.then_for_each("prices", "items") { |i| i.then_apply(s.price) }.combine(s.collect) }, "DYN_FORK").first["name"]
  end

  # ---------------------------------------------------------------- one_of / when / otherwise

  def test_one_of_guard_cascade_with_otherwise
    spec = define do |f, s|
      v = f.then_apply(s.validate)
      Wiggle.one_of(v.when(s.vip?).then_apply(s.concierge),
                    v.when(s.big?, "big-q").then_apply(s.standard),
                    v.otherwise.then_apply(s.thanks)).then_accept(s.notify)
    end
    vip, big = named(spec, "vip"), named(spec, "big")
    assert_equal vip["id"], named(spec, "validate")["next"]
    assert_equal big["id"], vip["altNext"]                       # vip false -> the next guard
    assert_equal named(spec, "thanks")["id"], big["altNext"]     # big false -> otherwise
    assert_equal named(spec, "concierge")["id"], vip["next"]
    assert_equal "big-q", big["queue"]
    %w[concierge standard thanks].each { |n| assert_equal named(spec, "notify")["id"], named(spec, n)["next"] }
  end

  def test_one_of_without_otherwise_skips_on_no_match
    spec = define { |f, s| Wiggle.one_of(f.when(s.vip?).then_apply(s.concierge)).then_apply(s.thanks) }
    assert_equal named(spec, "thanks")["id"], named(spec, "vip")["altNext"]
  end

  def test_one_of_validation
    assert_raises(ArgumentError) { define { |f, s| Wiggle.one_of(f.then_apply(s.concierge), f.otherwise.then_apply(s.thanks)) } }
    assert_raises(ArgumentError) { define { |f, s| Wiggle.one_of(f.otherwise.then_apply(s.thanks), f.when(s.vip?).then_apply(s.concierge)) } }
    assert_raises(ArgumentError) { define { |f, s| Wiggle.one_of(f.when(s.vip?), f.otherwise.then_apply(s.thanks)) } }
  end

  # ---------------------------------------------------------------- repeat_while

  def test_repeat_while_is_a_budgeted_cycle
    spec = define { |f, s| f.repeat_while(s.more?) { |body| body.then_apply(s.fetch) }.then_apply(s.validate) }
    cond, body = named(spec, "more"), named(spec, "fetch")
    assert_equal cond["id"], body["next"]
    assert_equal body["id"], cond["next"]
    assert_equal "validate", by_id(spec)[cond["altNext"]]["name"]
    assert_equal(-1, cond["loopBudget"])
    budgeted = define { |f, s| f.repeat_while(s.more?, 50, "loops") { |b| b.then_apply(s.fetch) } }
    assert_equal [50, "loops"], named(budgeted, "more").values_at("loopBudget", "queue")
    assert_raises(ArgumentError) { define { |f, s| f.repeat_while(s.more?, 0) { |b| b.then_apply(s.fetch) } } }
  end

  # ---------------------------------------------------------------- compensation

  def test_compensable_flag_serializes_only_when_declared
    spec = define { |f, s| f.then_apply_compensable(s.authorise).then_apply(s.charge) }
    assert_equal true, named(spec, "authorise")["compensable"]
    refute named(spec, "charge").key?("compensable") # absent, not false
  end

  # ---------------------------------------------------------------- conversions

  def test_value_round_trip_and_int_coercion
    obj = { "s" => "x", "n" => 3, "f" => 1.5, "b" => true, "none" => nil, "list" => [1, 2], "nested" => { "k" => 4 } }
    back = Convert.from_value(Convert.to_value(obj))
    assert_equal obj, back
    assert_kind_of Integer, back["n"]
    assert_equal({ "a" => "b" }, Convert.from_value(Convert.to_value({ a: :b })))
  end
end
