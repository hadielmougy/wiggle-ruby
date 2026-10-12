# frozen_string_literal: true

module Wiggle
  # A per-step retry policy. Durations are given in seconds (Ruby's stand-in for java.time.Duration);
  # the record itself keeps milliseconds, like the Java client's.
  RetryPolicy = Struct.new(:max_attempts, :initial_backoff_millis, :multiplier, :max_backoff_millis, :jitter) do
    def initialize(max_attempts, initial_backoff_millis, multiplier, max_backoff_millis, jitter)
      raise ArgumentError, "max_attempts must be >= 1" if max_attempts < 1
      raise ArgumentError, "multiplier must be >= 1.0" if multiplier < 1.0

      super
      freeze
    end

    # 10,000 attempts, 1s apart (capped at a minute) -- the default for a step that names no policy.
    def self.forever = new(10_000, 1_000, 1.0, 60_000, 0.0)

    # A single attempt: no retries.
    def self.none = new(1, 0, 1.0, 0, 0.0)

    # Doubling backoff from +initial_backoff+ seconds, capped at 5 minutes, with 20% jitter.
    def self.exponential(max_attempts, initial_backoff)
      new(max_attempts, millis(initial_backoff), 2.0, 300_000, 0.2)
    end

    # A constant +backoff+ seconds between attempts.
    def self.fixed(max_attempts, backoff) = new(max_attempts, millis(backoff), 1.0, millis(backoff), 0.0)

    def self.millis(seconds) = (seconds * 1000).round

    # @api private -- the graph-schema JSON shape.
    def to_json_hash
      { "maxAttempts" => max_attempts, "initialBackoffMillis" => initial_backoff_millis,
        "multiplier" => multiplier.to_f, "maxBackoffMillis" => max_backoff_millis, "jitter" => jitter.to_f }
    end
  end
end
