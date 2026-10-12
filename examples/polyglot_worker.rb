# frozen_string_literal: true

# The payments team's worker -- in Ruby -- for a flow whose topology was authored in Java.
#
# It never re-declares the workflow. It binds ONE step, `charge`, to the `polyglot-order` graph by
# name; Worker#start reconciles that binding against the graph the Java author registered on the
# server, discovers that `charge` polls the `payments` queue, and serves it. Run the Java side first
# so the graph exists and instances are flowing:
#
#   ./gradlew :example:runPolyglot                 # terminal 1 (in the wiggle repo)
#   ruby -Ilib examples/polyglot_worker.rb         # terminal 2: this payments worker
require "wiggle"

Wiggle.logger.level = Logger::INFO

Wiggle::Client.open(ENV.fetch("WIGGLE_URL", "localhost:8080")) do |client|
  # No FlowSpec, no topology -- just implement `charge` by (workflow, step) name.
  # await_registration rides out the race if this starts before the Java author registers.
  worker = Wiggle::Worker.new(client, "rb-payments", await_registration: 30)
                         .handle("polyglot-order", "charge") do |ctx|
    puts "   [rb-payments] charging order #{ctx['orderId']}"
    ctx.merge("paymentRef" => "auth-#{ctx['orderId']}")
  end
  puts "[rb-payments] serving `charge` on the payments queue; Ctrl-C to stop"
  worker.run_forever
end
