# frozen_string_literal: true

module Wiggle
  # Ambient metadata for the step currently running on this thread (fiber-local).
  #
  # A worker publishes the running task's identity around each handler call, so a handler can read
  # +Wiggle::Current.attempt+, +.step_name+ or +.instance_id+ without extra parameters, and can
  # +Wiggle::Current.emit(...)+ domain events that ride the step's report.
  #
  # Inside a forEach item step, the handler's parameter is the ITEM's current value; the frozen
  # pre-forEach context and the element's position/source key are available here instead of being
  # injected into user data:
  #
  #   worker.handle("charge", "price") do |item|
  #     { "sku" => item, "cost" => Wiggle::Current.base["rate"] * 2, "i" => Wiggle::Current.item_index }
  #   end
  #
  # +base+ is read-only by contract: an item can never write the base; only the forEach combine's
  # return reaches the shared context. Calling these outside a running step raises.
  module Current
    KEY = :__wiggle_step_scope

    # The per-attempt state a worker publishes around one handler call.
    Scope = Struct.new(:attempt, :step_name, :instance_id, :base, :has_base, :item, :index, :map_key,
                       :emitted, keyword_init: true) do
      def initialize(**kw)
        super
        self.attempt ||= 0
        self.index ||= 0
        self.emitted ||= []
      end
    end

    class << self
      # Emit a domain event onto the instance's event log. It is buffered and rides this step's
      # report, where the server appends it in the transaction that settles the step: committed if
      # and only if this attempt completes. An attempt that raises leaves nothing behind, and its
      # retry emits fresh. +type+ names the fact (the +wf.+ prefix is reserved for the engine);
      # +payload+ is a Hash, so a consumer can read a field from it.
      def emit(type, payload)
        scope = current!
        raise ArgumentError, "an event needs a type" if type.nil? || type.to_s.empty?
        unless payload.is_a?(Hash)
          raise TypeError, "event '#{type}' carries a #{payload.nil? ? 'nil' : payload.class} payload; " \
                           "an event's payload is a Hash, so a consumer can read a field from it"
        end

        scope.emitted << [type.to_s, payload]
        nil
      end

      # The engine-global attempt number: 1 on the first try, incremented on every retry.
      def attempt = current!.attempt

      # The running step's name.
      def step_name = current!.step_name

      # The workflow instance the running step belongs to.
      def instance_id = current!.instance_id

      # The frozen base context: inside a forEach item step the pre-forEach context; inside a
      # fork/forEach combine the pre-fork context (staged results excluded).
      def base
        scope = Thread.current[KEY]
        unless scope&.has_base
          raise Error, "Wiggle::Current.base is only available inside a forEach item step or a " \
                       "fork/forEach combine"
        end
        scope.base
      end

      # This element's position in the input collection.
      def item_index = require_item!.index

      # This element's source key when the input collection was a map; nil for a list.
      def item_map_key = require_item!.map_key

      # True while a step is running on this thread.
      def active? = !Thread.current[KEY].nil?

      # @api private -- run +blk+ with +scope+ published; restores the previous scope afterwards.
      def with_scope(scope)
        prev = Thread.current[KEY]
        Thread.current[KEY] = scope
        yield
      ensure
        Thread.current[KEY] = prev
      end

      private

      def current!
        Thread.current[KEY] || raise(Error, "Wiggle::Current metadata is only available inside a running step")
      end

      def require_item!
        scope = Thread.current[KEY]
        unless scope&.item
          raise Error, "Wiggle::Current.item_index/item_map_key are only available inside a forEach item step"
        end
        scope
      end
    end
  end
end
