# frozen_string_literal: true

# Like polyglot_worker.rb, but the handlers are the methods of one object: each public method is
# matched to a step of the Java-authored `polyglot-order` graph by name, regardless of case style.
#
#   ruby -Ilib examples/polyglot_typed_worker.rb
require "wiggle"

class PaymentHandlers < Wiggle::Handlers
  workflow "polyglot-order"

  # matches the step `charge`; returns the new context
  def charge(ctx)
    puts "   [rb-payments] charging order #{ctx['orderId']}"
    ctx.merge("paymentRef" => "auth-#{ctx['orderId']}")
  end
end

Wiggle::Client.open(ENV.fetch("WIGGLE_URL", "localhost:8080")) do |client|
  Wiggle::Worker.new(client, "rb-payments-typed", await_registration: 30)
                .register_handlers(PaymentHandlers.new)
                .run_forever
end
