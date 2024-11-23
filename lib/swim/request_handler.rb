require 'socket'
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

        begin
          Timeout.timeout(timeout) do
            socket = TCPSocket.new(host, port)
            request = {
              type: :request,
              path: path,
              payload: payload
            }.to_msgpack

            Logger.debug("Writing request to socket")
            socket.write([request.bytesize].pack('N'))  # Write message size first
            socket.write(request)
            
            # Read response size
            size_data = socket.read(4)
            return { error: 'Invalid response' } unless size_data
            
            size = size_data.unpack('N')[0]
            response_data = socket.read(size)
            
            Logger.debug("Reading response from socket")
            result = MessagePack.unpack(response_data)
            Logger.debug("Received response: #{result}")
            result
          ensure
            socket.close
          end
        rescue Timeout::Error => e
          Logger.error("Request timed out after #{timeout} seconds")
          { error: 'Request timed out' }
        rescue Errno::ECONNREFUSED => e
          Logger.error("Connection refused to #{host}:#{port}")
          { error: 'Connection refused' }
        rescue => e
          Logger.error("Error sending request: #{e.message}\n#{e.backtrace.join("\n")}")
          { error: e.message }
        end
      end

      def handle_connection(socket, service)
        Logger.debug("Handling new connection from #{socket.peeraddr[2]}:#{socket.peeraddr[1]}")
        
        begin
          # Read response size
          size_data = socket.read(4)
          return { error: 'Invalid response' } unless size_data
          
          size = size_data.unpack('N')[0]
          request_data = socket.read(size)
          
          Logger.debug("Received request: #{request_data}")
          request = MessagePack.unpack(request_data)
          path = request['path']
          payload = request['payload']
          
          response = service.handle_request(path, payload)
          Logger.debug("Sending response: #{response}")
          
          socket.write([response.to_msgpack.bytesize].pack('N'))  # Write message size first
          socket.write(response.to_msgpack)
        rescue => e
          Logger.error("Error handling connection: #{e.message}\n#{e.backtrace.join("\n")}")
          socket.write({ error: e.message }.to_msgpack)
        ensure
          socket.close
        end
      end
    end
  end
end
