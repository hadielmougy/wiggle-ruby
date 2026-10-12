# frozen_string_literal: true

require "logger"
require "timeout"

# Wiggle -- Ruby client and worker for the Wiggle workflow engine, in the Java client's vocabulary.
#
#   require "wiggle"
#
#   order = Wiggle::FlowSpec.define("order-fulfilment", 1, OrderSteps) do |f, s|
#     f.then_apply(s.validate).then_filter(s.in_stock?).then_accept(s.notify_customer)
#   end
#
# Define a workflow as a chain on a WiggleFlow handle, register it with a WiggleClient, and serve
# its steps from a Worker that binds handler objects by name. Talks the same gRPC control plane as
# the Java, Python and Go clients, so their workers interoperate.
module Wiggle
  class << self
    # The library's logger (WARN and above to stderr by default). Assign your own to integrate.
    attr_writer :logger

    def logger
      @logger ||= Logger.new($stderr, level: Logger::WARN, progname: "wiggle")
    end
  end
end

require_relative "wiggle/version"
require_relative "wiggle/errors"
require_relative "wiggle/convert"
require_relative "wiggle/retry_policy"
require_relative "wiggle/activity"
require_relative "wiggle/step"
require_relative "wiggle/topology"
require_relative "wiggle/flow"
require_relative "wiggle/client"
require_relative "wiggle/binder"
require_relative "wiggle/worker"
