# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "wiggle"
require "minitest/autorun"

Wiggle.logger = Logger.new(nil)

module TestSupport
  def by_id(spec) = spec.definition["nodes"].to_h { |n| [n["id"], n] }

  def of_kind(spec, kind) = spec.definition["nodes"].select { |n| n["kind"] == kind }

  def named(spec, name) = spec.definition["nodes"].find { |n| n["name"] == name } || flunk("no node named #{name}")
end
