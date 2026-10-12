# Wiggle — Ruby client

A Ruby client and worker for [Wiggle](https://github.com/hadielmougy/wiggle), the durable
state-machine platform, using the **Java client's vocabulary**: `FlowSpec.define`, a `WiggleFlow`
chain of `then_apply` / `then_filter` / `then_accept`, `Wiggle.all_of(...).combine(...)`, a
`WiggleClient`, and a `Worker` you `register_handler` on. It speaks the same gRPC control plane, so
**Ruby, Java, Python and Go workers interoperate** on the same server: any worker that serves a
step's name can run it.

The names are the Java ones in Ruby's snake_case (`thenApply` → `then_apply`, `isRunning` →
`running?`). Durations are seconds.

## Install

```ruby
# Gemfile
gem "wiggle-client"
```

Requires Ruby 3.1+ and a running Wiggle server — see the [engine repo](https://github.com/hadielmougy/wiggle)
(`docker run --rm -p 8080:8080 hadielmougy/wiggle`, or `./gradlew :dist:run` there).

## Quick start

```ruby
require "wiggle"

# 1. The contract: the steps, by name (Java: an interface). Bodies are irrelevant.
module OrderSteps
  extend Wiggle::ForFlow

  def validate(order) = nil
  def in_stock?(order) = nil
  def charge(order) = nil
  def reserve(order) = nil
  def settle(payment, shipping, order) = nil
  void def notify_customer(order) = nil      # void: an effect, its return is ignored
end

# 2. The topology, named through the contract's stand-in `s`. Nothing runs here.
order = Wiggle::FlowSpec.define("order-fulfilment", 1, OrderSteps) do |f, s|
  validated = f.then_apply(s.validate).then_filter(s.in_stock?)

  payment  = validated.then_apply(s.charge, "payments", Wiggle::RetryPolicy.exponential(5, 0.1))
  shipping = validated.then_apply(s.reserve).then_sleep(2)

  Wiggle.all_of(payment, shipping).combine(s.settle).then_accept(s.notify_customer)
end

# 3. The logic: a handler object that includes the same contract (Java: @ForFlow class).
class OrderHandlers
  extend Wiggle::ForFlow
  include OrderSteps
  for_flow "order-fulfilment"

  def validate(order) = order.merge("status" => "VALIDATED")
  def in_stock?(order) = order["quantity"].positive?
  def charge(order) = { "paymentRef" => "auth-#{order['orderId']}" }
  def reserve(order) = { "slot" => 7 }
  def settle(payment, shipping, order) = order.merge(payment).merge(shipping)
  def notify_customer(order) = puts("shipped #{order['orderId']}")
end

client = Wiggle::WiggleClient.new("localhost:8080")
client.register(order)
worker = Wiggle::Worker.new(client, "worker-1").register_handler(OrderHandlers.new).start

id = client.start(order, { "orderId" => "A-1", "quantity" => 3 })
view = client.await_completion(id, 30)
puts view.status, view.context        # COMPLETED {...}

worker.close
client.close
```

Run the bundled example against a server on `:8080`:

```bash
ruby -Ilib examples/order.rb          # WIGGLE_URL=host:port to point elsewhere
```

## Defining a flow

`FlowSpec.define(name, version, [default_retry], [contract]) { |f, s| … }` runs the block once and
records a chain. The block must return the handle it ends on. The version is **declared, not
derived**: a published version is immutable, so a changed graph needs a new number.

Steps are named by **reference**, never by running anything. With a contract module, `s.validate`
names the step `validate`. Without one, the block takes only `f`, and you pass a method object
(`handlers.method(:validate)`) or a Symbol (`:validate`). A trailing `?`/`!` is not part of the node
name, so `s.in_stock?` names the step `in_stock`. Blocks and lambdas are refused: they have no
handler method for a worker to bind.

| `WiggleFlow` method | Records |
|---|---|
| `then_apply(ref, [retry], [queue])` (alias `apply`) | a task: the handler's return becomes the new context |
| `then_apply_compensable(ref, …)` (alias `apply_compensable`) | a task with an undo (served by a `CompensableActivity`) |
| `then_accept(ref, …)` | an effect: the handler returns nothing, the context is unchanged |
| `then_filter(ref, …)` | a guard: when it fails, the instance ends as `gated:<name>` (inside an arm: skips to the join) |
| `then_sleep(seconds)` / `then_sleep(name, seconds)` | a server-side timer; no worker is held |
| `then_await(signal, [timeout]) { \|esc\| … }` | wait for `client.signal(...)`; on timeout fail, or run the escalation block and rejoin |
| `then_sub_flow(node, workflow)` | run another registered workflow as a child; its result merges back |
| `then_for_each([name,] items_key) { \|item\| … }.combine(ref)` | runtime fan-out, one isolated branch per element of the collection at `items_key` |
| `when(ref, …)` / `otherwise` + `Wiggle.one_of(*arms)` | exclusive choice: the first arm whose guard holds runs |
| `repeat_while(ref, [max_iterations], [queue]) { \|body\| … }` | do-while loop; the body runs at least once and the loop is budgeted |
| `default_queue(queue)` | the queue for every step recorded after this point |

Every step takes an optional `RetryPolicy` and an optional queue (a String), in either order, as in
Java. A step with no policy inherits the define's default (`RetryPolicy.forever`, 10,000 attempts).
A step with no queue uses the current `default_queue`, else the workflow name. Retry policies are
`RetryPolicy.exponential(attempts, initial_seconds)`, `RetryPolicy.fixed(attempts, seconds)`,
`RetryPolicy.none` and `RetryPolicy.forever`.

### Continuing a handle twice is a fan-out

`validated` above is continued twice, and that is exactly what makes the two arms. Each arm runs on
its **own isolated copy** of the context, so they must be rejoined:
`Wiggle.all_of(payment, shipping).combine(s.settle)`. There is no implicit merge. A split that is
never combined is rejected when the flow is defined:

```
in workflow 'order-fulfilment', the flow after 'validate' splits into ['charge', 'reserve'] but is never
rejoined. Continuing one handle twice is a fan-out: pass both to Wiggle.all_of(...) and combine them, or
continue only one of them.
```

An arm is named after its last step. The combine handler receives **one argument per arm, in the
order given to `all_of`**, plus optionally a trailing argument for the pre-fork context. It returns
the **complete** post-join context.

Ruby control flow is unrolled: the block runs once, so a loop around `then_apply` records that many
steps. Anything that depends on a step's *result* must be `Wiggle.one_of` or `repeat_while`, which
the engine evaluates at run time:

```ruby
Wiggle::FlowSpec.define("routing", 1, Steps) do |f, s|
  vip      = f.when(s.vip?).then_apply(s.concierge)
  standard = f.otherwise.then_apply(s.thanks)
  Wiggle.one_of(vip, standard).then_accept(s.notify)
end
```

### forEach

```ruby
Wiggle::FlowSpec.define("pricing", 1, Steps) do |f, s|
  f.then_for_each("items") { |item| item.then_apply(s.price) }.combine(s.total)
end
# start(spec, { "items" => [1, 2, 3], "rate" => 2 }) -> one isolated branch per element;
# an empty/missing collection skips the body AND the combine
```

**The element IS the item's context.** The body's handlers receive the element itself (scalars
included), and their return replaces it. The combine receives every item's final value: an Array in
item order, or a Hash keyed like the input when it was a map. It may also take the pre-forEach
context, and it returns the complete post-join context:

```ruby
def price(item) = item * Wiggle::Step.base["rate"]          # base: the frozen pre-forEach context
def total(prices, order) = order.merge("total" => prices.sum)
```

## Serving steps: handler objects

A worker binds a handler object's public methods to steps **by name, regardless of case style**:
`in_stock?`, `in_stock`, `inStock` and a Java-authored `in-stock` are the same step. The graph
decides whether a step is a task or a gate. The method's shape decides the rest:

| Method | Serves |
|---|---|
| `def validate(order)` | a task: the return **replaces** the context (`nil` leaves it untouched) |
| `def in_stock?(order)` on a gate node | a guard: its truthiness picks the branch |
| `void def notify(order)` | an effect: runs, and the return is ignored |
| `def settle(arm1, arm2, [base])` on a combine node | the merge: one argument per arm (or the forEach results), optionally the base |
| `def authorise` (no arguments) | a **factory** returning a `Wiggle::Activity` (`execute`), `GateActivity` (`test`), `EffectActivity` (`apply`) or `CompensableActivity` (`execute` + `compensate`) |

`void` and `handles` come from `extend Wiggle::ForFlow`, Ruby's rendering of Java's `void` and
`@Handles`. `void` can sit on the contract module that the handler includes.
`handles "capture-payment", def do_capture(o) … end` serves a step whose name differs from the
method's. `for_flow "name"` (Java's `@ForFlow`) names the workflow the object serves.

```ruby
worker.register_handler(OrderHandlers.new)                    # workflow from `for_flow`
worker.register_handler("order-fulfilment", PlainObject.new)  # or named here
worker.register_handler(OrderHandlersV3.new, 3)               # this version only
```

On `start`, each registration is checked against the registered graph. A method that matches no
step, or whose shape clashes with its node, fails fast. The worker also learns which queue each step
polls, so it needs no queue config. A version-scoped registration is checked against that exact
graph, and the worker claims only that version's tasks. If *any* registration is unversioned, the
worker claims every version.

### Compensation (sagas)

A step declared with `then_apply_compensable` is undone if the instance later fails. The handler
serves it with a factory returning a `CompensableActivity`. The engine runs `compensate` in the
reverse pass, handing it a `Wiggle::Compensation` with the step's `input` and `result` snapshots. The
instance settles `COMPENSATED` (or `COMPENSATION_FAILED`).

```ruby
class Authorise
  include Wiggle::CompensableActivity
  def execute(order) = gateway.charge(order)
  def compensate(c) = gateway.refund(c.result["reference"])   # idempotent: at-least-once
end

class OrderHandlers
  # ...
  def authorise = Authorise.new
end
```

### Step metadata and events

While a handler runs, `Wiggle::Step` describes it:

```ruby
def ship(order)
  Wiggle::Step.attempt        # 1 on the first try, incremented on every retry
  Wiggle::Step.name           # "ship"
  Wiggle::Step.instance_id
  Wiggle::Step.emit("order.shipped", { "orderId" => order["orderId"] })   # rides this step's report
  order
end
```

An emitted event is committed only if the attempt completes. `Step.base` is the frozen base context
inside a forEach item step or a combine, and `Step.item_index` / `Step.item_map_key` describe a
forEach element. Raise `Wiggle::PermanentActivityException` to fail a step without retrying.

## WiggleClient

```ruby
client = Wiggle::WiggleClient.new("localhost:8080", tls: nil, api_key: ENV["WIGGLE_API_KEY"])
client.register(flow_spec, force = false)
client.get_workflow(name, version = nil)          # -> Hash: the registered graph
client.workflow_names
client.start(flow_spec_or_name, context, version = nil, correlation_id = nil)   # -> instance id
client.instance(id)                               # -> InstanceView (#status #context #termination_reason #error #terminal?)
client.instance_detail(id)                        # -> InstanceWithTokens
client.await_completion(id, timeout)              # -> InstanceView, or raises Timeout::Error
client.list_instances(workflow, status, limit)
client.find_by_correlation(correlation_id, limit = 50)
client.cancel(id, reason)
client.signal(id, signal, payload)
client.create_schedule(workflow, every_seconds, context) / client.create_cron_schedule(workflow, cron, context)
client.schedules / client.delete_schedule(id)
client.poll_events(consumer, max, wait_millis, start_from) / client.ack_events(consumer, cursor_or_seq)
client.search(text, workflow, status, updated_from, updated_to, limit, partial_ok, semantic = false)
client.step_stats(workflow, version, since, sample) / client.backlog_coverage(max)
client.cluster
client.close
```

Every failure raises `Wiggle::WiggleApiException` with an HTTP-style `status`: 400 invalid, 401
unauthenticated, 403 forbidden, 404 not found, 409 conflict/precondition, 0 server unreachable, 500
otherwise. A call that finds the server UNAVAILABLE is retried first, `WIGGLE_RPC_MAX_ATTEMPTS`
times (default 5) with exponential backoff from `WIGGLE_RPC_RETRY_DELAY_MILLIS` (default 200).
`tls:` takes a `GRPC::Core::ChannelCredentials` (TLS/mTLS), or `true` for the system roots. The API
key is sent as `authorization: Bearer <key>`.

## Worker

```ruby
options = Wiggle::WorkerOptions.defaults
  .with_concurrency(8)              # in-flight handlers (default: CPU count)
  .with_lease(30)                   # seconds; renewed by heartbeat while a handler runs
  .with_long_poll_wait(10)
  .with_queues("payments")          # override the queues learned from the graph
  .with_await_registration(30)      # ride out a startup race with the authoring service
worker = Wiggle::Worker.new(client, "worker-1", options)
worker.register_handler(OrderHandlers.new).start
worker.running? / worker.in_flight
worker.close                        # stop polling and drain claimed tasks
```

Log output goes to `Wiggle.logger`; replace it with your own `Logger`.

## Java → Ruby names

| Java | Ruby |
|---|---|
| `FlowSpec.define("o", 1, Order.class, OrderSteps.class, (f, s) -> …)` | `Wiggle::FlowSpec.define("o", 1, OrderSteps) { \|f, s\| … }` |
| `f.thenApply(s::validate)` | `f.then_apply(s.validate)` |
| `thenFilter` / `thenAccept` / `thenApplyCompensable` | `then_filter` / `then_accept` / `then_apply_compensable` |
| `thenSleep(Duration.ofSeconds(2))` | `then_sleep(2)` |
| `thenAwait` / `thenSubFlow` / `thenForEach` / `repeatWhile` | `then_await` / `then_sub_flow` / `then_for_each` / `repeat_while` |
| `Wiggle.allOf(a, b).combine(s::settle)` / `Wiggle.oneOf(...)` | `Wiggle.all_of(a, b).combine(s.settle)` / `Wiggle.one_of(...)` |
| `interface OrderSteps` / `@ForFlow` / `@Handles` / `void` | contract `module` / `for_flow` / `handles` / `void` |
| `Activity` / `GateActivity` / `EffectActivity` / `CompensableActivity` | same names, as mixins (`execute` / `test` / `apply` / `compensate`) |
| `new WiggleClient(target)` / `awaitCompletion(id, Duration)` | `WiggleClient.new(target)` / `await_completion(id, seconds)` |
| `worker.registerHandler(h)` / `isRunning()` / `close()` | `register_handler(h)` / `running?` / `close` |
| `Step.attempt()` / `Step.emit(...)` / `Step.base()` | `Wiggle::Step.attempt` / `.emit(...)` / `.base` |
| `PermanentActivityException` / `WiggleApiException` | same names |

## Tests

```bash
bundle install
bundle exec rake                                         # offline tests (no server needed)
WIGGLE_TEST_URL=localhost:8080 bundle exec rake          # + end-to-end tests against a live server
```

## Notes & limits

- **Not ported:** run-time branches (`Step.create` and the combine that follows a creating step),
  queue criteria and attributes, the `LOCAL_SYNC`/`LOCAL_ASYNC` execution modes (and `checkpoint`),
  `@Decode`, and typed contexts. A Ruby context is a plain Hash (or, inside a forEach body, the
  element itself). Ruby-defined workflows run in `SERVER` mode.
- **Numbers:** context travels as protobuf `Value` (doubles). Whole numbers come back as `Integer`,
  and fractional values as `Float`. Hash keys come back as Strings.
- **Regenerating stubs:** `gem install grpc-tools && ./codegen.sh` after syncing `proto/wiggle.proto`
  with the engine's copy.
