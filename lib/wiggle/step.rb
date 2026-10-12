# frozen_string_literal: true

module Wiggle
  # Ambient metadata for the step running on this thread -- the Java client's Step. A worker
  # publishes the running task's identity around each handler call, so a handler can read it without
  # extra parameters, and can emit domain events that ride the step's report.
  #
  #   def ship(order)
  #     Wiggle::Step.emit("order.shipped", { "orderId" => order["orderId"], "attempt" => Wiggle::Step.attempt })
  #     order
  #   end
  module Step
    KEY = :__wiggle_step_info

    # The running step, all fields at once.
    Info = Struct.new(:attempt, :name, :instance_id, :base, :item_scope, :item_index, :item_map_key, :emitted)

    class << self
      # Emit a domain event onto the instance's event log. It is buffered and rides this step's
      # report, where the server appends it in the transaction that settles the step: committed if
      # and only if this attempt completes. +type+ names the fact (+wf.+ is reserved for the engine);
      # +payload+ is a Hash, so a consumer can read a field from it.
      def emit(type, payload)
        info = current
        raise ArgumentError, "an event needs a type" if type.nil? || type.to_s.empty?
        unless payload.is_a?(Hash)
          raise ArgumentError, "event '#{type}' carries a #{payload.nil? ? 'nil' : payload.class} payload; an " \
                               "event's payload is a Hash, so a consumer can read a field from it"
        end

        info.emitted << [type.to_s, payload]
        nil
      end

      # The engine-global attempt number: 1 on the first try, incremented on every retry.
      def attempt = current.attempt

      # The running step's name.
      def name = current.name

      # The workflow instance the running step belongs to.
      def instance_id = current.instance_id

      # The frozen base context: inside a forEach item step the pre-forEach context; inside a fork
      # combine the pre-fork context; inside a forEach combine the pre-forEach context. Read-only by
      # contract: only a handler's return reaches the engine.
      def base
        info = current
        if info.base.nil?
          raise Error, "Step.base is only available inside a forEach item step or a fork/forEach combine " \
                       "(elsewhere the context IS the handler's parameter)"
        end
        info.base
      end

      # Inside a forEach item step: this element's position in the input collection.
      def item_index = require_item_scope.item_index

      # Inside a forEach item step over a MAP input: this element's source key; nil for a list.
      def item_map_key = require_item_scope.item_map_key

      # The full snapshot.
      def current
        Thread.current[KEY] || raise(Error, "Step metadata is only available inside a running activity")
      end

      # @api private -- run the block with +info+ published.
      def with(info)
        prev = Thread.current[KEY]
        Thread.current[KEY] = info
        yield
      ensure
        Thread.current[KEY] = prev
      end

      private

      def require_item_scope
        info = current
        raise Error, "Step.item_index/item_map_key are only available inside a forEach item step" unless info.item_scope

        info
      end
    end

    # Misuse of Step outside the scope it describes.
    class Error < StandardError; end
  end
end
