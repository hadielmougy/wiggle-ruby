# frozen_string_literal: true

module Wiggle
  # The handler-side step types -- Ruby renderings of the Java client's Activity interfaces. A
  # handler serves a step either with a plain one-argument method, or with a zero-argument FACTORY
  # method of the step's name returning one of these:
  #
  #   class OrderHandlers
  #     include OrderSteps
  #     def authorise = Authorise.new        # serves the compensable step "authorise"
  #   end
  #
  #   class Authorise
  #     include Wiggle::CompensableActivity
  #     def execute(order) = gateway.charge(order)
  #     def compensate(c) = gateway.refund(c.result["reference"])
  #   end

  # A task: +execute(ctx)+ returns the new context.
  module Activity
    def execute(ctx) = raise(NotImplementedError, "#{self.class} must implement execute(ctx)")
  end

  # A guard: +test(ctx)+ returns whether the flow continues.
  module GateActivity
    def test(ctx) = raise(NotImplementedError, "#{self.class} must implement test(ctx)")
  end

  # A side effect: +apply(ctx)+ runs, and the context is unchanged.
  module EffectActivity
    def apply(ctx) = raise(NotImplementedError, "#{self.class} must implement apply(ctx)")
  end

  # An undo: +compensate(compensation)+ reverses what the step did.
  module Compensable
    def compensate(compensation) = raise(NotImplementedError, "#{self.class} must implement compensate(compensation)")
  end

  # A step that carries its own undo: Activity#execute does the work, Compensable#compensate
  # reverses it if the instance later fails. This is how a step is DECLARED compensable on the
  # handler side; WiggleFlow#then_apply_compensable declares it in the topology.
  module CompensableActivity
    include Activity
    include Compensable
  end

  # The pair of snapshots a compensator is given: what the step was handed (+input+) and what it
  # returned (+result+). Both are needed because a step's return REPLACES the context, so the
  # identifier an undo needs may live on either side.
  Compensation = Struct.new(:input, :result)
end
