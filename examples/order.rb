# frozen_string_literal: true

# End-to-end example: define an order-fulfilment workflow, run a worker, submit orders.
#
#   # start a server first, e.g. `docker run --rm -p 8080:8080 hadielmougy/wiggle`
#   ruby -Ilib examples/order.rb
require "wiggle"

# The contract: the steps, by name. The FlowSpec names steps through it, and the handlers include
# it, so the two cannot drift.
module OrderSteps
  extend Wiggle::ForFlow

  def validate(order) = nil
  def in_stock?(order) = nil
  def charge(order) = nil
  def reserve(order) = nil
  def settle(payment, shipping, order) = nil
  void def notify_customer(order) = nil
end

ORDER = Wiggle::FlowSpec.define("rb-order", 1, OrderSteps) do |f, s|
  validated = f.then_apply(s.validate).then_filter(s.in_stock?)

  payment  = validated.then_apply(s.charge, "payments", Wiggle::RetryPolicy.exponential(5, 0.1))
  shipping = validated.then_apply(s.reserve)

  Wiggle.all_of(payment, shipping).combine(s.settle).then_accept(s.notify_customer)
end

class OrderHandlers
  extend Wiggle::ForFlow
  include OrderSteps
  for_flow "rb-order"

  def validate(order) = order.merge("status" => "VALIDATED")
  def in_stock?(order) = order["quantity"].positive?
  def charge(order) = { "paymentRef" => "auth-#{order['orderId']}" }
  def reserve(order) = { "trackingLabel" => "DHL-#{order['orderId']}" }

  # one argument per arm, in all_of order, then the pre-fork context
  def settle(payment, shipping, order) = order.merge(payment).merge(shipping)

  def notify_customer(order)
    puts "   [worker] #{order['orderId']} -> #{order['status']} paid=#{order['paymentRef']} " \
         "tracking=#{order['trackingLabel']}"
  end
end

client = Wiggle::WiggleClient.new(ENV.fetch("WIGGLE_URL", "localhost:8080"))
client.register(ORDER)
worker = Wiggle::Worker.new(client, "rb-worker-1").register_handler(OrderHandlers.new).start
begin
  ids = 5.times.map { |i| client.start(ORDER, { "orderId" => "A-#{1000 + i}", "quantity" => i % 3 }) }
  ids.each do |id|
    view = client.await_completion(id, 30)
    line = "  #{id}  #{view.status}"
    line += "  (#{view.termination_reason})" if view.termination_reason
    line += "  ERROR: #{view.error}" if view.error
    puts line
  end
ensure
  worker.close
  client.close
end
