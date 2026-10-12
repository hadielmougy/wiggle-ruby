# frozen_string_literal: true

# The payments team's worker -- in Ruby -- for a flow whose topology was authored in Java.
#
# It never re-declares the workflow. Its handler object serves ONE step, `charge`, of the
# `polyglot-order` graph; Worker#start matches it to the graph the Java author registered,
# discovers that `charge` polls the `payments` queue, and serves it. Run the Java side first so the
# graph exists and instances are flowing:
#
#   ./gradlew :example:runPolyglot                 # terminal 1 (in the wiggle repo)
#   ruby -Ilib examples/polyglot_worker.rb         # terminal 2: this payments worker
require "wiggle"

Wiggle.logger.level = Logger::INFO

class PaymentHandlers
  extend Wiggle::ForFlow
  for_flow "polyglot-order"

  def charge(order)
    puts "   [rb-payments] charging order #{order['orderId']}"
    order.merge("paymentRef" => "auth-#{order['orderId']}")
  end
end

client = Wiggle::WiggleClient.new(ENV.fetch("WIGGLE_URL", "localhost:8080"))
# with_await_registration rides out the race if this starts before the Java author registers
options = Wiggle::WorkerOptions.defaults.with_await_registration(30)
worker = Wiggle::Worker.new(client, "rb-payments", options).register_handler(PaymentHandlers.new).start
puts "[rb-payments] serving `charge` on the payments queue; Ctrl-C to stop"
begin
  sleep
rescue Interrupt
  worker.close
end
