# frozen_string_literal: true

require_relative "test_helper"

# Offline tests for the topology and wire conversions -- no server required. They build workflows as
# declarative Graphs (and with the block builder) and assert on the compiled definition: node kinds,
# edge wiring, queues, version determinism.
class WorkflowTest < Minitest::Test
  include Wiggle
  include TestSupport

  # ---------------------------------------------------------------- linear + gate

  def test_linear_chain_and_gated_end
    spec = Graph.new("wf", [Step.new("a"), Gate.new("g"), Effect.new("b")]).compile
    a, g, b = named(spec, "a"), named(spec, "g"), named(spec, "b")
    assert_equal a["id"], spec.definition["startNode"]
    assert_equal "TASK", a["kind"]
    assert_equal g["id"], a["next"]
    assert_equal "PREDICATE", g["kind"]
    assert_equal b["id"], g["next"]
    assert_equal "wf#a", a["activity"]
    gated = by_id(spec)[g["altNext"]] # the gate's false path ends the instance as gated:g
    assert_equal "END", gated["kind"]
    assert_equal "gated:g", gated["reason"]
  end

  def test_default_queue_is_workflow_name_and_per_step_override
    spec = Graph.new("orders", [Step.new("a"), Step.new("b", queue: "payments")]).compile
    assert_equal "orders", named(spec, "a")["queue"]
    assert_equal "payments", named(spec, "b")["queue"]
    assert_equal %w[orders payments], spec.queues
  end

  def test_graph_default_queue
    spec = Graph.new("orders", [Step.new("a")], default_queue: "shared").compile
    assert_equal "shared", named(spec, "a")["queue"]
  end

  def test_retry_json_shape
    spec = Graph.new("wf", [Step.new("a", retry: Retry.exponential(5, 0.1))]).compile
    assert_equal({ "maxAttempts" => 5, "initialBackoffMillis" => 100, "multiplier" => 2.0,
                   "maxBackoffMillis" => 300_000, "jitter" => 0.2 }, named(spec, "a")["retry"])
    assert_equal 1, Retry.none.max_attempts
    assert_equal 2_000, Retry.fixed(3, 2).to_json_hash["maxBackoffMillis"]
  end

  # ---------------------------------------------------------------- versions

  def test_version_is_deterministic_and_positive_int32
    build = -> { Graph.new("wf", [Step.new("a"), Gate.new("g")]).compile.version }
    assert_equal build.call, build.call
    assert_includes 1..0x7FFFFFFF, build.call
  end

  def test_version_is_structural_independent_of_node_ids
    spec = Graph.new("wf", [
      Step.new("a"),
      Fork.new([Branch.new("l", [Step.new("l1")]), Branch.new("r", [Step.new("r1")])], combine: "merge"),
      Step.new("z")
    ]).compile
    # relabel every node id to a totally different scheme, rewriting all edge references too
    remap = spec.definition["nodes"].each_with_index.to_h { |n, i| [n["id"], "x#{i}"] }
    relabelled = spec.definition.merge(
      "startNode" => remap[spec.definition["startNode"]],
      "nodes" => spec.definition["nodes"].map do |n|
        m = n.merge("id" => remap[n["id"]])
        %w[next altNext].each { |e| m[e] = remap[m[e]] if m.key?(e) }
        m["branches"] = m["branches"].map { |b| remap[b] } if m.key?("branches")
        m
      end
    )
    assert_equal spec.version, Versioning.content_version(relabelled), "hash depends on structure, not ids"
  end

  def test_version_changes_when_structure_changes
    refute_equal Graph.new("wf", [Step.new("a")]).compile.version,
                 Graph.new("wf", [Step.new("a"), Step.new("b")]).compile.version
  end

  # The canonical encoding matches the Python client's, so the same topology registers as the same
  # version from either language. These numbers were produced by wiggle-python's Graph.compile().
  def test_version_matches_the_python_client
    order = Graph.new("order", [
      Step.new("validate"), Gate.new("in-stock"),
      Fork.new([Branch.new("payment", [Step.new("charge", queue: "payments", retry: Retry.exponential(5, 0.1))]),
                Branch.new("shipping", [Step.new("reserve"), Step.new("label")])], combine: "merge"),
      Effect.new("notify")
    ])
    assert_equal 801_371_869, order.compile.version

    everything = Graph.new("wf", [
      ForEach.new("each", over: "items", body: [Step.new("price")], combine: "collect"),
      Choose.new([Case.on("vip", [Step.new("v")]), Case.otherwise([Step.new("s")])]),
      DoWhile.new("again", [Step.new("b")], max_iterations: 7),
      Sleep.new("nap", millis: 250),
      AwaitSignal.new("approval", timeout: 60, escalation: [Step.new("auto")]),
      SubWorkflow.new("call", "child"),
      Step.new("ünï😀", compensate: true, retry: Retry.fixed(3, 0.5))
    ], default_queue: "q")
    assert_equal 36_447_000, everything.compile.version

    floats = Graph.new("big", [Step.new("a", retry: Retry.new(max_attempts: 10, initial_backoff: 1e-05, multiplier: 1.5,
                                                               max_backoff: 1e20, jitter: 0.123456789))])
    assert_equal 311_082_877, floats.compile.version
  end

  def test_explicit_version_override_and_validation
    spec = Graph.new("wf", [Step.new("a")], version: 42).compile
    assert_equal 42, spec.version
    assert_equal 42, spec.definition["version"]
    assert_raises(ArgumentError) { Graph.new("wf", [Step.new("a")], version: 0).compile }
    assert_raises(ArgumentError) { Graph.new("wf", [Step.new("a")], version: 0x80000000).compile }
    assert_raises(TypeError) { Graph.new("wf", [Step.new("a")], version: "3").compile }
  end

  def test_empty_workflow_rejected
    err = assert_raises(ArgumentError) { Graph.new("wf", []).compile }
    assert_match(/no steps/, err.message)
  end

  # ---------------------------------------------------------------- timers / signals

  def test_sleep_and_signal_nodes
    spec = Graph.new("wf", [Sleep.new("nap", millis: 250), AwaitSignal.new("go", timeout: 2), Step.new("done")]).compile
    nap, go = named(spec, "nap"), named(spec, "go")
    assert_equal ["SLEEP", 250], [nap["kind"], nap["sleepMillis"]]
    assert_equal ["SIGNAL", 2000], [go["kind"], go["sleepMillis"]]
    refute nap.key?("activity") # timers/signals carry no activity (no worker handler)
    refute go.key?("activity")
  end

  def test_await_signal_escalation_branch_wiring
    spec = Graph.new("wf", [
      Step.new("request"),
      AwaitSignal.new("approval", timeout: 60, escalation: [Step.new("auto-approve")]),
      Step.new("finish")
    ]).compile
    sig, auto, finish = named(spec, "approval"), named(spec, "auto-approve"), named(spec, "finish")
    assert_equal 60_000, sig["sleepMillis"]
    assert_equal finish["id"], sig["next"]    # delivery continues to `finish`
    assert_equal auto["id"], sig["altNext"]   # timeout escalates to the branch
    assert_equal finish["id"], auto["next"]   # escalation rejoins the flow at `finish`
    assert_equal "wf#auto-approve", auto["activity"]
  end

  def test_await_signal_without_escalation_has_no_alt_edge
    spec = Graph.new("wf", [Step.new("a"), AwaitSignal.new("s", timeout: 5), Step.new("b")]).compile
    refute named(spec, "s").key?("altNext")
  end

  def test_await_signal_escalation_validation
    err = assert_raises(ArgumentError) { Graph.new("wf", [AwaitSignal.new("s", escalation: [Step.new("x")])]).compile }
    assert_match(/positive timeout/, err.message)
    err = assert_raises(ArgumentError) { Graph.new("wf", [AwaitSignal.new("s", timeout: 5, escalation: [])]).compile }
    assert_match(/defines no steps/, err.message)
  end

  # ---------------------------------------------------------------- fork / join

  def test_fork_creates_fork_join_and_mandatory_combine
    spec = Graph.new("wf", [
      Fork.new([Branch.new("l", [Step.new("l1")]), Branch.new("r", [Step.new("r1"), Step.new("r2")])], combine: "merge"),
      Step.new("after")
    ]).compile
    fork = of_kind(spec, "FORK").first
    join = of_kind(spec, "JOIN").first
    assert_equal 2, fork["branches"].size
    assert_equal 2, join["expected"]
    fork["branches"].each { |start| assert by_id(spec).key?(start) }
    # the join flows into the mandatory combine (a TASK carrying the arm names), and only then into
    # "after" -- there is no implicit fold
    combine = named(spec, "merge")
    assert_equal "TASK", combine["kind"]
    assert_equal %w[l r], combine["armNames"]
    assert_equal combine["id"], join["next"]
    assert_equal named(spec, "after")["id"], combine["next"]
  end

  def test_fork_validation
    assert_raises(ArgumentError) { Graph.new("wf", [Fork.new([Branch.new("only", [Step.new("x")])], combine: "m")]).compile }
    err = assert_raises(ArgumentError) do
      Graph.new("wf", [Fork.new([Branch.new("l", [Step.new("l1")]), Branch.new("r", [Step.new("r1")])])]).compile
    end
    assert_match(/combine/, err.message)
  end

  def test_gate_inside_a_branch_short_circuits_to_the_join
    spec = Graph.new("wf", [
      Fork.new([Branch.new("l", [Gate.new("ok"), Step.new("l1")]), Branch.new("r", [Step.new("r1")])], combine: "m")
    ]).compile
    assert_equal of_kind(spec, "JOIN").first["id"], named(spec, "ok")["altNext"]
  end

  # ---------------------------------------------------------------- forEach (dynamic)

  def test_for_each_dynfork_join_and_mandatory_combine
    spec = Graph.new("wf", [
      ForEach.new("each", over: "items", body: [Step.new("price")], combine: "collect"),
      Step.new("sum")
    ]).compile
    df = of_kind(spec, "DYN_FORK").first
    join = of_kind(spec, "JOIN").first
    assert_equal "items", df["itemsKey"]
    refute_empty df["branches"]
    assert_equal join["id"], df["next"] # empty-collection skip -> join
    refute join.key?("expected")        # dynamic width, not a fixed count
    combine = named(spec, "collect")
    assert_equal "TASK", combine["kind"]
    assert_equal "__forEach__each", combine["collectKey"]
    assert_equal combine["id"], join["next"]
    assert_equal named(spec, "sum")["id"], combine["next"]
  end

  def test_for_each_requires_a_combine
    err = assert_raises(ArgumentError) { Graph.new("wf", [ForEach.new("each", over: "items", body: [Step.new("p")])]).compile }
    assert_match(/combine/, err.message)
  end

  # ---------------------------------------------------------------- choose

  def test_choose_guard_cascade_with_otherwise
    spec = Graph.new("wf", [
      Choose.new([Case.on("vip", [Step.new("v")]), Case.on("big", [Step.new("g")]), Case.otherwise([Step.new("s")])]),
      Step.new("after")
    ]).compile
    vip, big = named(spec, "vip"), named(spec, "big")
    assert_equal big["id"], vip["altNext"] # vip false -> the next guard
    assert_equal "TASK", by_id(spec)[big["altNext"]]["kind"] # big false -> the otherwise branch
    assert_equal %w[big vip], of_kind(spec, "PREDICATE").map { |n| n["name"] }.sort
  end

  def test_choose_without_otherwise_skips_on_no_match
    spec = Graph.new("wf", [Choose.new([Case.on("vip", [Step.new("v")])]), Step.new("after")]).compile
    assert_equal named(spec, "after")["id"], named(spec, "vip")["altNext"]
  end

  def test_choose_rejects_otherwise_not_last
    assert_raises(ArgumentError) do
      Graph.new("wf", [Choose.new([Case.otherwise([Step.new("s")]), Case.on("vip", [Step.new("v")])])]).compile
    end
  end

  # ---------------------------------------------------------------- do_while

  def test_do_while_is_a_cycle
    spec = Graph.new("wf", [DoWhile.new("again", [Step.new("body")]), Step.new("done")]).compile
    cond, body = named(spec, "again"), named(spec, "body")
    assert_equal cond["id"], body["next"] # body tail feeds the condition
    assert_equal body["id"], cond["next"] # true edge loops back to the body
    assert_equal "done", by_id(spec)[cond["altNext"]]["name"]
    assert_equal(-1, cond["loopBudget"]) # unstated budget = engine-default sentinel
  end

  def test_do_while_explicit_budget
    spec = Graph.new("wf", [DoWhile.new("again", [Step.new("body")], max_iterations: 500)]).compile
    assert_equal 500, named(spec, "again")["loopBudget"]
    refute named(Graph.new("wf", [Gate.new("g"), Step.new("a")]).compile, "g").key?("loopBudget")
    err = assert_raises(ArgumentError) { Graph.new("wf", [DoWhile.new("x", [Step.new("b")], max_iterations: -2)]).compile }
    assert_match(/max_iterations/, err.message)
  end

  # ---------------------------------------------------------------- sub-workflow

  def test_sub_workflow_carries_child_name
    child = Graph.new("child", [Step.new("x")]).compile
    spec = Graph.new("parent", [Step.new("prep"), SubWorkflow.new("call", child)]).compile
    sub = of_kind(spec, "SUB_WORKFLOW").first
    assert_equal %w[child call], [sub["activity"], sub["name"]]
    assert_equal "child", of_kind(Graph.new("p2", [SubWorkflow.new("c", "child")]).compile, "SUB_WORKFLOW").first["activity"]
  end

  # ---------------------------------------------------------------- validation

  def test_duplicate_step_name_rejected
    assert_raises(ArgumentError) { Graph.new("wf", [Step.new("a"), Step.new("a")]).compile }
  end

  def test_compensate_flag_serializes_only_when_set
    spec = Graph.new("saga", [Step.new("reserve", compensate: true), Effect.new("audit", compensate: true),
                              Step.new("plain")]).compile
    assert_equal true, named(spec, "reserve")["compensable"]
    assert_equal true, named(spec, "audit")["compensable"]
    refute named(spec, "plain").key?("compensable") # absent, not false: keeps existing content hashes
  end

  # ---------------------------------------------------------------- block builder

  def test_block_builder_builds_the_same_definition
    declarative = Graph.new("order", [
      Step.new("validate"),
      Gate.new("in-stock"),
      Fork.new([Branch.new("payment", [Step.new("charge", queue: "payments", retry: Retry.exponential(5, 0.1))]),
                Branch.new("shipping", [Step.new("reserve"), Step.new("label")])], combine: "merge"),
      Choose.new([Case.on("vip", [Step.new("concierge")]), Case.otherwise([Step.new("thanks")])]),
      ForEach.new("each", over: "items", body: [Step.new("price")], combine: "collect"),
      DoWhile.new("again", [Step.new("poll")], max_iterations: 3),
      Sleep.new("cool-off", seconds: 1),
      AwaitSignal.new("approval", timeout: 60, escalation: [Step.new("auto")]),
      SubWorkflow.new("call", "child"),
      Effect.new("notify", compensate: true)
    ]).compile

    built = Wiggle.workflow("order") do
      step "validate"
      gate "in-stock"
      fork combine: "merge" do
        branch("payment") { step "charge", queue: "payments", retry: Wiggle::Retry.exponential(5, 0.1) }
        branch("shipping") do
          step "reserve"
          step "label"
        end
      end
      choose do
        on("vip") { step "concierge" }
        otherwise { step "thanks" }
      end
      for_each("each", over: "items", combine: "collect") { step "price" }
      do_while("again", max_iterations: 3) { step "poll" }
      sleep "cool-off", seconds: 1
      await_signal("approval", timeout: 60) { step "auto" }
      sub_workflow "call", "child"
      effect "notify", compensate: true
    end

    assert_equal declarative.definition, built.definition
    assert_equal declarative.version, built.version
  end

  def test_block_builder_yields_when_the_block_takes_a_parameter
    outer = "from-outer-scope"
    spec = Wiggle.workflow("wf") { |f| f.step(outer) }
    assert_equal "wf##{outer}", named(spec, outer)["activity"]
    assert_instance_of Graph, Wiggle.workflow("wf", compile: false) { step "a" }
  end

  # ---------------------------------------------------------------- conversions

  def test_value_round_trip_and_int_coercion
    obj = { "s" => "x", "n" => 3, "f" => 1.5, "b" => true, "none" => nil, "list" => [1, 2], "nested" => { "k" => 4 } }
    back = Convert.from_value(Convert.to_value(obj))
    assert_equal obj, back
    assert_kind_of Integer, back["n"]
    assert_kind_of Float, back["f"]
  end

  def test_symbols_are_stringified_on_the_wire
    assert_equal({ "a" => "b" }, Convert.from_value(Convert.to_value({ a: :b })))
    assert_raises(TypeError) { Convert.to_value(Object.new) }
  end
end
