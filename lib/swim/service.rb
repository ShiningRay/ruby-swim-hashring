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
      @hash_ring = HashRing.new(["#{host}:#{port}"])
      @handlers = {}

      Logger.info("Initializing service #{name} on #{host}:#{port}")
      setup_state_handlers
    end

    def start
      Logger.info("Starting service #{@name}")
      @protocol.start
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
      target_node = @hash_ring.get_node(service_name)
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
        response = RequestHandler.send_request(host, port.to_i, path, payload)
        Logger.debug("Received response from #{node}: #{response}")
        response
      rescue => e
        Logger.error("Error forwarding request to #{node}: #{e.message}")
        { error: "Failed to forward request: #{e.message}" }
      end
    end

    def update_ring
      # Update hash ring with current alive members
      nodes = @protocol.members.values
                      .select(&:alive?)
                      .map(&:address)
      Logger.debug("Updating hash ring with nodes: #{nodes.join(', ')}")
      @hash_ring = HashRing.new(nodes)
    end
  end
end
