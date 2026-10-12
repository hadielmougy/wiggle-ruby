# frozen_string_literal: true

module Wiggle
  # A control-plane call failed. +status+ is HTTP-style, like the Java client's WiggleApiException:
  # 400 invalid argument, 401 unauthenticated, 403 permission denied, 404 not found, 409 failed
  # precondition / already exists, 0 server unreachable, 500 anything else. +cause+ is the
  # underlying GRPC::BadStatus.
  class WiggleApiException < StandardError
    attr_reader :status

    def initialize(status, message)
      super(message)
      @status = status
    end

    def client_error? = status >= 400 && status < 500
  end

  # Raise from a handler to fail the step WITHOUT retrying (a bad request, not a blip).
  class PermanentActivityException < StandardError; end
end
