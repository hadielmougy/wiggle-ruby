# frozen_string_literal: true

require "logger"

# Wiggle -- Ruby client and worker for the Wiggle workflow engine.
#
#   require "wiggle"
#
# Describe a workflow's topology as declarative data (a Graph of nodes, or the Wiggle.workflow block
# builder), compile and register it, start instances from the client, and implement its steps with a
# worker that binds handlers by name. Talks the same gRPC control plane as the Java, Python and Go
# clients, so their workers interoperate.
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
require_relative "wiggle/current"
require_relative "wiggle/workflow"
require_relative "wiggle/dsl"
require_relative "wiggle/client"
require_relative "wiggle/binder"
require_relative "wiggle/worker"
