require 'socket'
require 'msgpack'
require_relative 'protocol'
require_relative 'hash_ring'
require_relative 'request_handler'
require_relative 'logger'

module Swim
  class Service
    attr_reader :name, :host, :port

    def initialize(name, host, port, seeds = [])
      @name = name
      @host = host
      @port = port
      @seeds = seeds
      @protocol = Protocol.new(host, port, seeds, self)
      @handlers = {}
      @running = false
      @http_server = nil
      @http_port = port + 1000  # HTTP 端口比 UDP 端口大 1000

      Logger.info("Initializing service #{name} on #{host}:#{@port} (HTTP: #{@http_port})")
      setup_state_handlers
    end

    def start
      return if @running
      @running = true
      Logger.info("Starting service #{@name}")
      @protocol.start
      start_http_server
    end

    def stop
      return unless @running
      @running = false
      @protocol.stop
      stop_http_server
    end

    def register_handler(path, &block)
      Logger.debug("Registering handler for path: #{path}")
      @handlers[path] = block
    end

    def handle_request(path, payload)
      handler = @handlers[path]
      unless handler
        Logger.warn("No handler found for path: #{path}")
        return { error: 'Not Found' }
      end
      
      begin
        Logger.debug("Handling request for path: #{path} with payload: #{payload}")
        result = handler.call(payload)
        Logger.debug("Request handled successfully: #{result}")
        result
      rescue => e
        Logger.error("Error handling request: #{e.message}\n#{e.backtrace.join("\n")}")
        { error: e.message }
      end
    end

    def route_request(service_name, path, payload)
      target_node = @protocol.hash_ring.get_node(service_name)
      unless target_node
        Logger.error("No available node found for service: #{service_name}")
        return { error: 'Service Unavailable' }
      end

      if target_node == "#{@host}:#{@port}"
        Logger.debug("Handling local request for #{service_name}#{path}")
        handle_request(path, payload)
      else
        Logger.debug("Forwarding request to #{target_node} for #{service_name}#{path}")
        forward_request(target_node, path, payload)
      end
    end

    def set_state(key, value)
      Logger.debug("Setting state: #{key} = #{value}")
      @protocol.state_manager.set(key, value)
    end

    def get_state(key)
      value = @protocol.state_manager.get(key)
      Logger.debug("Getting state: #{key} = #{value}")
      value
    end

    def delete_state(key)
      Logger.debug("Deleting state: #{key}")
      @protocol.state_manager.delete(key)
    end

    private

    def setup_state_handlers
      Logger.debug("Setting up state handlers")
      # Register state-related handlers
      register_handler('/state/get') do |payload|
        key = payload['key']
        value = get_state(key)
        { status: 'success', key: key, value: value }
      end

      register_handler('/state/set') do |payload|
        key = payload['key']
        value = payload['value']
        set_state(key, value)
        { status: 'success', key: key, value: value }
      end

      register_handler('/state/delete') do |payload|
        key = payload['key']
        delete_state(key)
        { status: 'success', key: key }
      end
    end

    def forward_request(node, path, payload)
      host, port = node.split(':')
      Logger.debug("Forwarding request to #{node}#{path}")
      begin
        socket = UDPSocket.new
        request = {
          path: path,
          payload: payload
        }.to_msgpack
        
        socket.send(request, 0, host, port.to_i)
        
        response = socket.recvfrom(65535)[0]
        MessagePack.unpack(response)
      rescue => e
        Logger.error("Error forwarding request to #{node}: #{e.message}")
        { error: "Failed to forward request: #{e.message}" }
      ensure
        socket.close
      end
    end

    def start_http_server
      require 'webrick'
      @http_server = WEBrick::HTTPServer.new(
        Port: @http_port,
        Logger: WEBrick::Log.new("/dev/null"),
        AccessLog: []
      )

      @http_server.mount_proc '/' do |req, res|
        begin
          path = req.path
          payload = req.body ? JSON.parse(req.body) : {}
          
          Logger.debug("HTTP Request: #{req.request_method} #{path}")
          Logger.debug("Payload: #{payload}")
          
          result = handle_request(path, payload)
          
          res.status = result[:error] ? 400 : 200
          res.content_type = 'application/json'
          res.body = result.to_json
        rescue => e
          Logger.error("HTTP Error: #{e.message}\n#{e.backtrace.join("\n")}")
          res.status = 500
          res.content_type = 'application/json'
          res.body = { error: e.message }.to_json
        end
      end

      Thread.new do
        Logger.info("Starting HTTP server on port #{@http_port}")
        @http_server.start
      end
    end

    def stop_http_server
      if @http_server
        Logger.info("Stopping HTTP server")
        @http_server.shutdown
        @http_server = nil
      end
    end
  end
end
