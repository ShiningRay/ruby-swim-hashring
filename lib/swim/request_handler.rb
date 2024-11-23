require 'async'
require 'async/io'
require 'async/io/protocol/line'
require 'msgpack'
require 'timeout'
require_relative 'logger'

module Swim
  class RequestHandler
    DEFAULT_TIMEOUT = 5 # seconds

    class << self
      def send_request(host, port, path, payload, timeout = DEFAULT_TIMEOUT)
        Logger.debug("Sending request to #{host}:#{port}#{path}")
        Logger.debug("Request payload: #{payload}")

        Async do |task|
          begin
            endpoint = Async::IO::Endpoint.tcp(host, port)
            peer = endpoint.connect
            stream = Async::IO::Protocol::Line.new(peer)

            request = {
              path: path,
              payload: payload
            }

            Logger.debug("Writing request to socket")
            stream.write_line(request.to_msgpack)
            stream.flush
            
            Logger.debug("Reading response from socket")
            response = stream.read_line
            result = MessagePack.unpack(response)
            Logger.debug("Received response: #{result}")
            result
          rescue Async::TimeoutError => e
            Logger.error("Request timed out after #{timeout} seconds")
            { error: 'Request timed out' }
          rescue Errno::ECONNREFUSED => e
            Logger.error("Connection refused to #{host}:#{port}")
            { error: 'Connection refused' }
          rescue => e
            Logger.error("Error sending request: #{e.message}\n#{e.backtrace.join("\n")}")
            { error: e.message }
          ensure
            peer&.close
          end
        end
      end

      def handle_connection(peer, service)
        Logger.debug("Handling new connection")
        
        Async do |task|
          begin
            stream = Async::IO::Protocol::Line.new(peer)
            request_data = stream.read_line
            request = MessagePack.unpack(request_data)
            
            Logger.debug("Received request: #{request}")
            path = request['path']
            payload = request['payload']
            
            response = service.handle_request(path, payload)
            Logger.debug("Sending response: #{response}")
            
            stream.write_line(response.to_msgpack)
            stream.flush
          rescue => e
            Logger.error("Error handling connection: #{e.message}\n#{e.backtrace.join("\n")}")
            stream.write_line({ error: e.message }.to_msgpack)
            stream.flush
          ensure
            peer.close
          end
        end
      end
    end
  end
end
