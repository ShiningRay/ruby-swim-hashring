require 'concurrent'
require 'msgpack'
require 'securerandom'
require 'socket'
require 'logger'

require_relative 'swim/version'
require_relative 'swim/logger'
require_relative 'swim/member'
require_relative 'swim/state_manager'
require_relative 'swim/protocol'
require_relative 'swim/http_server'

module Swim
  class Error < StandardError; end
  
  # Create a new node in the cluster
  # @param host [String] The host address for this node
  # @param port [Integer] The port number for this node
  # @param seeds [Array<String>] Array of seed nodes in the format ["host:port", ...]
  # @param initial_metadata [Hash] Initial metadata for the node
  # @param enable_http [Boolean] Whether to enable the HTTP monitoring server
  # @param http_port [Integer, nil] Optional specific port for the HTTP server
  # @return [Swim::Protocol] The protocol instance managing this node
  def self.create_node(host, port, seeds = [], initial_metadata = {}, enable_http: false, http_port: nil)
    protocol = Protocol.new(host, port, seeds, initial_metadata)
    
    if enable_http
      http_server = HttpServer.new(protocol, http_port)
      protocol.instance_variable_set(:@http_server, http_server)
      
      original_start = protocol.method(:start)
      protocol.define_singleton_method(:start) do
        original_start.call
        @http_server.start
      end
      
      original_stop = protocol.method(:stop)
      protocol.define_singleton_method(:stop) do
        @http_server.stop
        original_stop.call
      end
    end
    
    protocol
  end
end
