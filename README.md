# Wiggle — Ruby client

An idiomatic Ruby client and worker for [Wiggle](https://github.com/hadielmougy/wiggle), the durable
state-machine platform. It speaks the same gRPC control plane as the Java client, so **Ruby, Java,
Python and Go workers interoperate** on the same server: define a flow in any of them, and any worker
that registers the matching handlers can run its steps.

- **Control client**: register workflows, start and track instances, deliver signals, manage
  schedules, and read the event feed, search, step statistics and backlog coverage.
- **Worker**: implement steps by name. It pulls only the tasks it has capacity for, runs handlers,
  and reports results, with automatic lease heartbeats and retries.
- **Declarative topology**: describe a workflow as a `Graph` of nodes (`Step`/`Gate`/`Sleep`/`Fork`/…)
  that mirror the graph schema, or with the equivalent `Wiggle.workflow` block builder.

## Install

```ruby
# Gemfile
gem "wiggle-client"
```

```bash
bundle install
# or from a checkout:
bundle install && bundle exec rake
```

Requires Ruby 3.1+ and a running Wiggle server. See the [engine repo](https://github.com/hadielmougy/wiggle)
(`docker run --rm -p 8080:8080 hadielmougy/wiggle`, or `./gradlew :dist:run` there).

## Quick start

```ruby
require "wiggle"

# 1. Describe the topology. It is pure data that mirrors the graph schema.
wf = Wiggle.workflow("order") do
  step "validate"
  gate "in-stock"                                    # false -> ends as gated:in-stock
  step "charge", queue: "payments", retry: Wiggle::Retry.exponential(5, 0.1)
  sleep "cool-off", seconds: 1
  effect "notify"
end

# 2. Implement the steps by name. The context is a plain Hash; a step returns the whole new context.
def bind(worker)
  worker
    .handle("order", "validate") { |o| o.merge("status" => "VALIDATED") }
    .handle_gate("order", "in-stock") { |o| o["quantity"].positive? }
    .handle("order", "charge") { |o| o.merge("paymentRef" => "auth-#{o['orderId']}") }
    .handle_effect("order", "notify") { |o| puts "shipped #{o['orderId']}" }
end

Wiggle::Client.open("localhost:8080") do |client|
  client.register(wf)
  bind(Wiggle::Worker.new(client, "worker-1")).start do       # background threads; stops after the block
    id = client.start(wf, { "orderId" => "A-1", "quantity" => 3 })
    view = client.await_completion(id, timeout: 30)
    puts view.status, view.context                             # COMPLETED {...}
  end
end
```

Run the bundled example against a server on `:8080`:

```bash
ruby -Ilib examples/order.rb          # WIGGLE_URL=host:port to point elsewhere
```

## Topology

A workflow's shape is declarative data: a `Wiggle::Graph.new(name, steps)` whose `steps` is an Array
of node values that mirror the graph schema. `compile` validates the shape and returns a `FlowSpec`
you register. Handlers are **not** part of the topology. A worker binds them separately, by name
(see below).

| Node | Meaning |
|---|---|
| `Step.new(name, queue:, retry:, compensate:)` | a task run on a worker (`handle`); its return replaces the context. `compensate: true` declares an undo that runs in the reverse pass if the instance later fails |
| `Effect.new(name, queue:, retry:, compensate:)` | a side-effect step (`handle_effect`); context unchanged |
| `Gate.new(name, queue:, retry:)` | a predicate (`handle_gate`); false ends the instance as `gated:<name>` |
| `Fork.new([Branch.new(name, steps), …], combine:)` | run branches **in parallel** on isolated context copies, then rejoin at the **mandatory** `combine` step (`handle_combine`). There is no implicit fold; needs ≥ 2 branches |
| `ForEach.new(name, over:, body:, combine:)` | runtime fan-out: one **isolated** branch per element of the list (or map) at `over`. **The element IS the item's context**: body handlers receive the item's value (scalars included), their return replaces it, and the frozen base rides on `Wiggle::Current.base`. The mandatory `combine` handler receives every item's final value under `name` and returns the complete post-join context |
| `Choose.new([Case.on(guard, steps), …, Case.otherwise(steps)])` | exclusive choice: the first case whose guard holds runs; `otherwise` (last) is the default |
| `DoWhile.new(condition, body, max_iterations:)` | run `body`, then repeat while the `condition` predicate holds (body runs at least once; the loop is budgeted) |
| `SubWorkflow.new(name, workflow)` | run another workflow (a `FlowSpec`, `Graph`, or name) as a child; its result merges back |
| `Sleep.new(name, seconds:, millis:)` | server-side timer; no worker is held |
| `AwaitSignal.new(name, timeout:, escalation:)` | wait for a signal via `client.signal(...)`; on timeout, fail, or run the `escalation` nodes and rejoin |

The block builder produces **exactly** the same definition (and version). The two forms are
interchangeable:

```ruby
include Wiggle

wf = Graph.new("order", [
  Step.new("validate"),
  Fork.new([                                              # parallel, joined
    Branch.new("payment",  [Step.new("charge")]),
    Branch.new("shipping", [Step.new("reserve"), Step.new("label")]),
  ], combine: "merge"),          # mandatory: branches rejoin at an explicit merge handler
  Choose.new([                                            # exactly one arm runs
    Case.on("vip", [Step.new("concierge")]),
    Case.otherwise([Step.new("thanks")]),
  ]),
  Effect.new("notify"),
]).compile

wf = Wiggle.workflow("order") do
  step "validate"
  fork combine: "merge" do
    branch("payment")  { step "charge" }
    branch("shipping") { step "reserve"; step "label" }
  end
  choose do
    on("vip") { step "concierge" }
    otherwise { step "thanks" }
  end
  effect "notify"
end
```

Inside the builder, `for_each(name, over:, combine:) { … }`, `do_while(cond, max_iterations:) { … }`,
`await_signal(name, timeout:) { escalation… }`, `sleep(name, seconds:)` and `sub_workflow(name, wf)`
cover the remaining nodes. A block that takes a parameter is yielded the builder instead of being
`instance_eval`'d, so the outer `self` stays reachable: `Wiggle.workflow("wf") { |f| f.step "a" }`.

A fork's combine is bound with `handle_combine(workflow, step)`. It receives the context with each
branch's result under the branch's name, and must return the **complete** post-join context. The
engine replaces the context with it, so keys the handler omits do not survive the join (there is no
implicit union of the arms):

```ruby
worker.handle_combine("order", "merge") do |ctx|
  ctx.except("payment", "shipping")       # carry the base
     .merge(ctx["payment"] || {})         # fold what each arm produced
     .merge(ctx["shipping"] || {})
end
```

`Wiggle::Current.base` returns that base (the pre-fork context) inside a combine too. If a branch name
is also a key of the workflow's own context, the combine fails rather than guess.

Runtime fan-out spawns one ISOLATED branch per element (list or map). The element IS each item's
context: the handler receives the value itself, and its return replaces it:

```ruby
wf = Wiggle.workflow("charge") do
  for_each("prices", over: "items", combine: "collect") { step "price" }
  step "summarise"
end
# start(wf, { "items" => [1, 2, 3], "rate" => 2 }) -> one branch per item;
# an empty/missing collection skips the body AND the combine

worker.handle("charge", "price") { |item| item * Wiggle::Current.base["rate"] }   # base is read-only
worker.handle_combine("charge", "collect") do |ctx|
  prices = ctx.delete("prices")   # every item's FINAL value: a list for a list input, a map for a map
  ctx.merge("prices" => prices)   # the COMPLETE post-join context -- it replaces
end
```

Per-step `queue` defaults to `default_queue:` (a `Graph` / `Wiggle.workflow` option), else the
workflow name. Retry policies (seconds): `Retry.exponential(attempts, initial)`,
`Retry.fixed(attempts, backoff)`, `Retry.none`, `Retry.forever` (the default). Raise
`Wiggle::PermanentError` from a handler to fail a step **without** retrying.

A `SubWorkflow`'s child must be registered separately (`client.register(child)`), and some worker must
serve the child's steps too. Because dispatch is by activity name (`"<workflow>#<step>"`), Ruby, Java,
Python and Go workers interoperate: any can run another's steps.

## Step metadata and events

While a handler runs, `Wiggle::Current` describes the running step:

```ruby
worker.handle("order", "ship") do |ctx|
  Wiggle::Current.attempt          # 1 on the first try, incremented on every retry
  Wiggle::Current.step_name        # "ship"
  Wiggle::Current.instance_id
  Wiggle::Current.emit("order.shipped", { "orderId" => ctx["orderId"] })   # rides this step's report
  ctx
end
```

An emitted event is committed only if the attempt completes; a failed attempt leaves nothing behind.
Read the feed with `client.poll_events(consumer)` / `client.ack_events(consumer, cursor:)`. Inside a
forEach item step, `Current.base`, `Current.item_index` and `Current.item_map_key` describe the item.

## Compensation (sagas)

A step declared `compensate: true` is undone when the instance later fails. The engine runs the
step's **compensator** in the reverse pass (newest-completed first) as a real durable task, and hands
it a `Wiggle::Compensation` holding the step's **input/result context snapshots** captured at
completion. These are not the instance's latest context, which a later step may have replaced. The
instance settles `COMPENSATED` (or `COMPENSATION_FAILED` if an undo exhausts its retries).

```ruby
worker.handle("order", "reserve") { |o| reserve(o) }
worker.handle_compensation("order", "reserve") { |c| release_reservation(c.result["reservationRef"]) }  # idempotent!
```

On a handlers object, the compensator is a method named `compensate_<step>` that takes the
`Compensation` (`compensate_reserve` undoes `reserve`). The pairing is checked both ways at start: a
compensable step served without its compensator, or a compensator targeting a non-compensable step,
refuses to bind. Compensators are at-least-once like every handler, so make them idempotent.

## Binding handlers by name (polyglot)

You don't have to author a workflow in Ruby to implement one of its steps in Ruby. If any client
(Java, Go, Python or Ruby) has already registered the graph, bind handlers **by name** without
re-declaring the topology:

```ruby
# implement just `charge` on a flow whose topology was authored elsewhere (e.g. in Java)
Wiggle::Worker.new(client, "payments")
  .handle("order-fulfilment", "charge") { |o| o.merge("paymentRef" => "auth-#{o['orderId']}") }
  .start
```

| Method | Binds a… |
|---|---|
| `handle(workflow, step) { \|ctx\| … }` | task: returns the new context (it replaces; `nil` leaves it untouched) |
| `handle_gate(workflow, step) { \|ctx\| … }` | predicate (a gate / choose guard / do-while condition); truthiness picks the branch |
| `handle_effect(workflow, step) { \|ctx\| … }` | side effect: runs, and the context is unchanged |
| `handle_combine(workflow, step) { \|ctx\| … }` | a fork/forEach combine: returns the complete post-join context |
| `handle_compensation(workflow, step) { \|comp\| … }` | the undo of a compensable step |

Each also accepts a callable instead of a block: `handle("wf", "s", method(:charge))`.

On `start` the worker **reconciles** every binding against the registered graph. It checks that each
step exists and is the right kind, so a typo or a task bound as a gate fails fast and lists the real
step names. It also **discovers which queue each step polls**, so a name-only worker needs no queue
config. The graph must be registered *before* the worker starts. Pass `await_registration: 30` to
ride out a startup race instead of failing fast. See
[`examples/polyglot_worker.rb`](examples/polyglot_worker.rb) for a Java-authored flow served from Ruby.

### A whole object of handlers: `register_handlers`

Instead of one `handle` call per step, hand the worker an object whose public methods *are* the steps:

```ruby
class OrderHandlers < Wiggle::Handlers
  workflow "order-fulfilment"             # or pass workflow: to register_handlers

  def validate(o) = o.merge("status" => "VALIDATED")      # a task: returns the new context

  def in_stock?(o) = o["qty"].positive?                    # matches the gate named "in-stock"

  effect def notify(o)                                     # a side effect: context unchanged
    puts "shipped #{o['orderId']}"
  end

  private

  def receipt(o) = ...                                     # private: a helper, not a step handler
end

Wiggle::Worker.new(client, "orders").register_handlers(OrderHandlers.new).start
```

Each public method is matched to a step **by name, regardless of case style**: `inStock`, `in_stock`,
`in_stock?` and a graph step named `in-stock` all fold to the same key. The graph is the source of
truth for the exact step name and for gate-vs-task, and a handler that contradicts it fails fast:
`effect` on a gate, or a `?`-named method on a task. Mark side effects with `effect` (it takes method
names, or wraps a `def`), or pass `effects: [:notify]` to `register_handlers` for a plain object. Two
methods whose names collide under case-folding are rejected at `register_handlers` time (ambiguous). A
method that matches no step is caught on `start`.

## Client API

```ruby
client = Wiggle::Client.new("localhost:8080")          # or Wiggle::Client.open(target) { |c| … }
client.register(flow_spec, force: false)               # -> version
client.get_workflow(name, version: nil)                # -> Hash: the registered graph
client.workflow_names
client.start(flow_spec_or_name, context, version: nil, correlation_id: nil)   # -> instance id
client.instance(id)                                    # -> InstanceView (#status #context #termination_reason #error #done?)
client.await_completion(id, timeout: 30)               # -> InstanceView, or raises Wiggle::TimeoutError
client.list_instances(workflow: nil, status: nil, correlation_id: nil, limit: 100)
client.cancel(id, reason: "cancelled")
client.signal(id, name, payload = nil)
client.create_schedule(workflow, every: 60 | cron: "*/5 * * * *", context: nil)
client.list_schedules / client.delete_schedule(id)
client.poll_events(consumer, max_events: 100, wait: 10, start_from: 0) / client.ack_events(consumer, cursor:)
client.search(text, workflow: nil, status: nil, semantic: false, …)
client.step_stats(workflow) / client.backlog_coverage
client.health / client.cluster
client.close
```

Connection options: `credentials:` (a `GRPC::Core::ChannelCredentials` for TLS/mTLS) or `tls: true`
(system trust roots). `api_key:` defaults to `ENV["WIGGLE_API_KEY"]` and is sent as
`authorization: Bearer <key>`, like the Java client. `wait_for_ready:` (default `true`) makes the
worker-critical RPCs (register, get_workflow, poll, report, fail, heartbeat) wait for an unreachable
server instead of failing, so a worker rides out a cold start or a rolling restart. Query calls stay
fail-fast. RPC failures surface as `GRPC::BadStatus` subclasses (`GRPC::NotFound`, …).

The worker takes `concurrency:` (default: CPU count), `lease:`, `long_poll_wait:`, `queues:` (overrides
discovery) and `await_registration:`, all in seconds. `start` runs it on background threads.
`start { … }` runs the block and then drains. `run_forever` blocks until SIGINT/SIGTERM. Log output
goes to `Wiggle.logger`, which you can replace with your own `Logger`.

## Versioning

By default a workflow's version is a **content hash of its structure**: node kinds, names, activities,
queues, retries and the edge topology. It does not depend on internal node-id numbering. So the same
structure always yields the same version: re-registering is idempotent, the server de-duplicates,
and changing the graph mints a new version (in-flight instances keep running on the old one). The
hash is byte-compatible with the Python client's, so the same topology declared in Ruby or Python
registers as the same version.

Pin an explicit version when you want a stable, human-meaningful one:

```ruby
wf = Wiggle.workflow("order", version: 3) { step "validate" }
```

With an explicit version **you** own bumping it when the graph changes. The server overwrites the
stored graph for a reused `name:version`, which affects instances already running on it.

## Tests

```bash
bundle install
bundle exec rake                                         # offline tests (no server needed)
WIGGLE_TEST_URL=localhost:8080 bundle exec rake          # + end-to-end tests against a live server
```

## Notes & limits

- **Execution mode:** Ruby-defined workflows run in `SERVER` mode. The worker does not implement the
  `LOCAL_SYNC`/`LOCAL_ASYNC` (client-side chaining) protocol; serve those with a Java worker. Run-time
  branches (`Step.create`) and queue criteria are not ported yet.
- **Numbers:** context travels as protobuf `Value` (doubles). Whole numbers come back as `Integer`,
  and fractional values as `Float`. Hash keys come back as Strings (Symbol keys and values are
  accepted on the way out).
- **Regenerating stubs:** `gem install grpc-tools && ./codegen.sh` after syncing `proto/wiggle.proto`
  with the engine's copy.
