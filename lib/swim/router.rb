require 'webrick'
require 'json'
require_relative 'logger'
require_relative 'protocol'

module Swim
  class Router
    def initialize(host, port, seeds = [])
      @host = host
      @port = port
      @protocol = Protocol.new(host, port, seeds)
      @services = {}
      @http_server = nil
      @http_port = port + 1000
      @running = false
    end

    def register_service(name, service)
      Logger.info("Registering service: #{name}")
      @services[name] = service
    end

    def start
      return if @running
      @running = true
      Logger.info("Starting router on #{@host}:#{@port}")
      @protocol.start
      start_http_server
    end

    def stop
      return unless @running
      @running = false
      @protocol.stop
      stop_http_server
    end

    private

    def start_http_server
      @http_server = WEBrick::HTTPServer.new(
        Port: @http_port,
        Logger: WEBrick::Log.new("/dev/null"),
        AccessLog: []
      )

      @http_server.mount_proc '/' do |req, res|
        handle_http_request(req, res)
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

    def handle_http_request(req, res)
      begin
        path_parts = req.path.split('/')
        service_name = path_parts[1]
        service_path = '/' + path_parts[2..-1].join('/')
        payload = req.body ? JSON.parse(req.body) : {}
        
        Logger.debug("HTTP Request: #{req.request_method} #{req.path}")
        Logger.debug("Service: #{service_name}, Path: #{service_path}")
        Logger.debug("Payload: #{payload}")
        
        result = route_request(service_name, service_path, payload)
        
        res.status = result[:error] ? 400 : 200
        res.content_type = 'application/json'
        res.body = JSON.generate(result)
      rescue => e
        Logger.error("HTTP Error: #{e.message}\n#{e.backtrace.join("\n")}")
        res.status = 500
        res.content_type = 'application/json'
        res.body = JSON.generate({ error: e.message })
      end
    end

    def route_request(service_name, path, payload)
      service = @services[service_name]
      unless service
        Logger.error("Service not found: #{service_name}")
        return { error: 'Service not found' }
      end

      begin
        service.handle_request(path, payload)
      rescue => e
        Logger.error("Error in service #{service_name}: #{e.message}")
        { error: e.message }
      end
    end
  end
end
