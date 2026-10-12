# frozen_string_literal: true

require_relative "lib/wiggle/version"

Gem::Specification.new do |spec|
  spec.name = "wiggle-client"
  spec.version = Wiggle::VERSION
  spec.authors = ["Hadi Elmougy"]
  spec.summary = "Ruby client and worker for the Wiggle workflow engine"
  spec.description = "Describe Wiggle workflows as declarative graphs, register and start them, and run " \
                     "their steps on Ruby workers that interoperate with Java, Python and Go workers."
  spec.homepage = "https://github.com/hadielmougy/wiggle-ruby"
  spec.license = "Apache-2.0"
  spec.required_ruby_version = ">= 3.1"

  spec.metadata = {
    "source_code_uri" => spec.homepage,
    "engine_uri" => "https://github.com/hadielmougy/wiggle",
    "rubygems_mfa_required" => "true"
  }

  spec.files = Dir["lib/**/*.rb", "proto/*.proto", "README.md", "LICENSE"]
  spec.require_paths = ["lib"]

  spec.add_dependency "google-protobuf", ">= 3.25", "< 5"
  spec.add_dependency "grpc", "~> 1.60"
  spec.add_dependency "logger"
end
