# frozen_string_literal: true

module Wiggle
  # Base class for every error this library raises on its own account. Transport failures surface as
  # the underlying GRPC::BadStatus subclasses (GRPC::NotFound, GRPC::Unavailable, ...).
  class Error < StandardError; end

  # Raise from a handler to fail the step WITHOUT retrying (a bad request, not a blip).
  class PermanentError < Error; end

  # #await_completion gave up before the instance reached a terminal state.
  class TimeoutError < Error; end

  # The server refused a reported run; it wrote nothing. +status+ is HTTP-style (404, 409, ...).
  class ReportRefused < Error
    attr_reader :task_id, :status

    def initialize(task_id, status, message)
      super("report of task #{task_id} refused (#{status}): #{message}")
      @task_id = task_id
      @status = status
    end
  end
end
