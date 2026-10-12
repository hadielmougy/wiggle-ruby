# frozen_string_literal: true

# End-to-end example: define an order-fulfilment workflow, run a worker, submit orders.
#
#   # start a server first, e.g. `docker run --rm -p 8080:8080 hadielmougy/wiggle`
#   ruby -Ilib examples/order.rb
require "wiggle"

# Declarative topology -- mirrors the graph schema. Handlers bind by name below.
FLOW = Wiggle.workflow("rb-order") do
  step "validate"
  gate "in-stock"
  step "charge", queue: "payments", retry: Wiggle::Retry.exponential(5, 0.1)
  step "ship"
  effect "notify"
end

# A worker implements steps by name; a step returns the whole new context.
def bind(worker)
  worker
    .handle("rb-order", "validate") { |o| o.merge("status" => "VALIDATED") }
    .handle_gate("rb-order", "in-stock") { |o| o["quantity"].positive? }
    .handle("rb-order", "charge") { |o| o.merge("paymentRef" => "auth-#{o['orderId']}") }
    .handle("rb-order", "ship") { |o| o.merge("trackingLabel" => "DHL-#{o['orderId']}") }
    .handle_effect("rb-order", "notify") do |o|
      puts "   [worker] #{o['orderId']} -> #{o['status']} paid=#{o['paymentRef']} tracking=#{o['trackingLabel']}"
    end
end

Wiggle::Client.open(ENV.fetch("WIGGLE_URL", "localhost:8080")) do |client|
  client.register(FLOW)
  worker = bind(Wiggle::Worker.new(client, "rb-worker-1")).start
  begin
    ids = 5.times.map { |i| client.start(FLOW, { "orderId" => "A-#{1000 + i}", "quantity" => i % 3 }) }
    ids.each do |id|
      view = client.await_completion(id, timeout: 30)
      line = "  #{id}  #{view.status}"
      line += "  (#{view.termination_reason})" if view.termination_reason
      line += "  ERROR: #{view.error}" if view.error
      puts line
    end
  ensure
    worker.stop
  end
end
