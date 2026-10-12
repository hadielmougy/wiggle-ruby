# frozen_string_literal: true

require "rake/testtask"

# Offline tests always run; the integration tests run too when WIGGLE_TEST_URL points at a server.
Rake::TestTask.new(:test) do |t|
  t.libs << "test"
  t.test_files = FileList["test/**/*_test.rb"]
  t.warning = false
end

desc "Regenerate the gRPC stubs from proto/wiggle.proto"
task :codegen do
  sh "./codegen.sh"
end

task default: :test
