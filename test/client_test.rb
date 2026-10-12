# frozen_string_literal: true

require_relative "test_helper"
require "socket"
require "stringio"

# Client behaviour against a tiny in-process gRPC server (no real Wiggle server needed): the
# cold-start wait, the API key header, and failure logging.
class ClientTest < Minitest::Test
  include Wiggle

  class FakeControlPlane < Wiggle::PB::WiggleControlPlane::Service
    attr_reader :seen_metadata

    def register_workflow(req, call)
      @seen_metadata = call.metadata
      Wiggle::PB::RegisterWorkflowResult.new(name: req.definition.fields["name"].string_value, version: "7", nodes: 1)
    end
  end

  def free_port
    s = TCPServer.new("127.0.0.1", 0)
    s.addr[1]
  ensure
    s&.close
  end

  def serve(port, service = FakeControlPlane.new)
    server = GRPC::RpcServer.new(pool_size: 2)
    server.add_http2_port("127.0.0.1:#{port}", :this_port_is_insecure)
    server.handle(service)
    thread = Thread.new { server.run }
    server.wait_till_running(5)
    [server, thread, service]
  end

  def flow = Graph.new("cold", [Step.new("a")]).compile

  # A worker-critical call issued before the server exists must WAIT for it and then succeed once it
  # comes up -- rather than failing fast with UNAVAILABLE.
  def test_cold_start_register_waits_for_server
    port = free_port
    client = Client.new("127.0.0.1:#{port}") # wait_for_ready defaults to true
    result = {}
    t = Thread.new do
      result[:version] = client.register(flow)
    rescue StandardError => e
      result[:error] = e
    end

    Kernel.sleep 0.6
    assert_empty result, "register returned before the server was up: #{result}"

    server, thread, = serve(port)
    begin
      t.join(10)
      assert_equal 7, result[:version], "register did not complete after the server came up: #{result}"
    ensure
      server.stop
      thread.join
      client.close
    end
  end

  def test_query_calls_stay_fail_fast
    client = Client.new("127.0.0.1:#{free_port}")
    assert_raises(GRPC::Unavailable) { client.instance("nope") }
    assert_raises(GRPC::Unavailable) { Client.new("127.0.0.1:#{free_port}", wait_for_ready: false).register(flow) }
  end

  def test_api_key_travels_as_a_bearer_token
    port = free_port
    server, thread, service = serve(port)
    begin
      Client.open("http://127.0.0.1:#{port}", api_key: " s3cret ") { |c| c.register(flow) }
      assert_equal "Bearer s3cret", service.seen_metadata["authorization"]
    ensure
      server.stop
      thread.join
    end
  end

  def test_failed_rpcs_are_logged
    port = free_port
    server, thread, = serve(port)
    log = StringIO.new
    begin
      client = Client.new("127.0.0.1:#{port}", logger: Logger.new(log))
      assert_raises(GRPC::Unimplemented) { client.get_workflow("nope") } # the fake doesn't implement it
      assert_match(/rpc get_workflow failed: Unimplemented/, log.string)
    ensure
      server.stop
      thread.join
    end
  end

  def test_create_schedule_and_ack_events_validate_arguments
    client = Client.new("127.0.0.1:1")
    assert_raises(ArgumentError) { client.create_schedule("wf") }
    assert_raises(ArgumentError) { client.create_schedule("wf", every: 1, cron: "* * * * *") }
    assert_raises(ArgumentError) { client.ack_events("c") }
  end
end
