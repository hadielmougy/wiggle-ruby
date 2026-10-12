# frozen_string_literal: true

require_relative "test_helper"
require "socket"

# WiggleClient against a tiny in-process gRPC server (no real Wiggle server needed): the transient
# retry, status mapping, and the API key header.
class ClientTest < Minitest::Test
  include Wiggle

  class FakeControlPlane < Wiggle::PB::WiggleControlPlane::Service
    attr_reader :seen_metadata

    def register_workflow(req, call)
      @seen_metadata = call.metadata
      Wiggle::PB::RegisterWorkflowResult.new(name: req.definition.fields["name"].string_value, version: "1", nodes: 1)
    end

    def get_workflow(req, _call) = raise(GRPC::NotFound, "no workflow #{req.name}")
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

  def flow = FlowSpec.define("cold", 1) { |f| f.then_apply(:a) }

  # A call issued while the server is briefly unreachable is retried (UNAVAILABLE means the RPC never
  # ran) and succeeds once the server comes up within the retry budget.
  def test_unavailable_is_retried_until_the_server_is_up
    port = free_port
    client = WiggleClient.new("127.0.0.1:#{port}")
    result = {}
    t = Thread.new do
      client.register(flow)
      result[:ok] = true
    rescue StandardError => e
      result[:error] = e
    end
    Kernel.sleep 0.5
    assert_empty result, "register gave up before the retry budget ran out: #{result}"

    server, thread, = serve(port)
    begin
      t.join(10)
      assert_equal({ ok: true }, result)
    ensure
      server.stop
      thread.join
      client.close
    end
  end

  def test_an_unreachable_server_is_status_0_once_retries_run_out
    ENV["WIGGLE_RPC_MAX_ATTEMPTS"] = "1"
    err = assert_raises(WiggleApiException) { WiggleClient.new("127.0.0.1:#{free_port}").instance("x") }
    assert_equal 0, err.status
    assert_kind_of GRPC::Unavailable, err.cause
  ensure
    ENV.delete("WIGGLE_RPC_MAX_ATTEMPTS")
  end

  def test_statuses_map_to_http_codes
    port = free_port
    server, thread, = serve(port)
    begin
      client = WiggleClient.new("127.0.0.1:#{port}")
      err = assert_raises(WiggleApiException) { client.get_workflow("nope") }
      assert_equal 404, err.status
      assert err.client_error?
      assert_match(/no workflow nope/, err.message)
      assert_equal 500, assert_raises(WiggleApiException) { client.workflow_names }.status # unimplemented
    ensure
      server.stop
      thread.join
    end
  end

  def test_api_key_travels_as_a_bearer_token
    port = free_port
    server, thread, service = serve(port)
    begin
      WiggleClient.new("http://127.0.0.1:#{port}", api_key: " s3cret ").register(flow)
      assert_equal "Bearer s3cret", service.seen_metadata["authorization"]
    ensure
      server.stop
      thread.join
    end
  end
end
