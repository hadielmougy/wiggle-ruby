# frozen_string_literal: true

require "google/protobuf/struct_pb"

module Wiggle
  # Conversions between Ruby values and the protobuf +Value+ used on the wire.
  #
  # Contexts are JSON-shaped: Hash keys come back as Strings (Symbol keys are accepted on the way out),
  # and since proto only has doubles, whole numbers come back as Integer so contexts read naturally.
  module Convert
    module_function

    # Convert a JSON-shaped Ruby object into a Google::Protobuf::Value.
    def to_value(obj)
      case obj
      when nil then Google::Protobuf::Value.new(null_value: :NULL_VALUE)
      when true, false then Google::Protobuf::Value.new(bool_value: obj)
      when Integer, Float then Google::Protobuf::Value.new(number_value: obj.to_f)
      when String then Google::Protobuf::Value.new(string_value: obj)
      when Symbol then Google::Protobuf::Value.new(string_value: obj.to_s)
      when Hash then Google::Protobuf::Value.new(struct_value: to_struct(obj))
      when Array
        Google::Protobuf::Value.new(list_value: Google::Protobuf::ListValue.new(values: obj.map { |v| to_value(v) }))
      else
        raise TypeError, "cannot serialise #{obj.class} to a wiggle context value"
      end
    end

    # Convert a Hash into a Google::Protobuf::Struct.
    def to_struct(hash)
      s = Google::Protobuf::Struct.new
      hash.each { |k, v| s.fields[k.to_s] = to_value(v) }
      s
    end

    # Convert a Google::Protobuf::Value back into a plain Ruby object (nil for an unset value).
    def from_value(v)
      return nil if v.nil?

      case v.kind
      when nil, :null_value then nil
      when :bool_value then v.bool_value
      when :number_value
        n = v.number_value
        n.finite? && n == n.floor ? n.to_i : n
      when :string_value then v.string_value
      when :struct_value then from_struct(v.struct_value)
      when :list_value then v.list_value.values.map { |item| from_value(item) }
      else raise ArgumentError, "unexpected value kind: #{v.kind}"
      end
    end

    # Convert a Google::Protobuf::Struct into a String-keyed Hash.
    def from_struct(s)
      return {} if s.nil?

      s.fields.each_with_object({}) { |(k, val), out| out[k] = from_value(val) }
    end
  end
end
