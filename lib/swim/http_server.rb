require 'webrick'
require 'json'

module Swim
  class HttpServer
    def initialize(protocol, port = nil)
      @protocol = protocol
      @port = port || (@protocol.port + 1000)
      @server = nil
      @start_time = Time.now.to_i
    end

    def start
      return if @server
      
      @server = WEBrick::HTTPServer.new(
        Port: @port,
        Logger: WEBrick::Log.new("/dev/null"),
        AccessLog: []
      )

      setup_routes
      
      Thread.new do
        Logger.info("Starting HTTP monitoring server on port #{@port}")
        @server.start
      end
    end

    def stop
      if @server
        Logger.info("Stopping HTTP monitoring server")
        @server.shutdown
        @server = nil
      end
    end

    private

    def setup_routes
      # GET /status - Basic node status
      @server.mount_proc '/status' do |_, res|
        data = {
          node: "#{@protocol.host}:#{@protocol.port}",
          uptime: Time.now.to_i - @start_time,
          alive_members_count: @protocol.alive_members.size,
          suspect_members_count: @protocol.suspect_members.size,
          dead_members_count: @protocol.dead_members.size
        }
        json_response(res, data)
      end

      # GET /members - List all members and their status
      @server.mount_proc '/members' do |_, res|
        members = @protocol.members.map do |member|
          {
            id: member.id,
            host: member.host,
            port: member.port,
            status: member.status,
            metadata: member.metadata,
            last_state_change: member.last_state_change,
            incarnation: member.incarnation
          }
        end
        json_response(res, { members: members })
      end

      # GET /metadata - Get node metadata
      @server.mount_proc '/metadata' do |_, res|
        json_response(res, { metadata: @protocol.metadata })
      end

      # GET /state - Get distributed state
      @server.mount_proc '/state' do |req, res|
        namespace = req.query['namespace'] || 'default'
        json_response(res, { 
          namespace: namespace,
          state: @protocol.state_manager.get_namespace(namespace) 
        })
      end
    end

    def json_response(res, data)
      res.status = 200
      res.content_type = 'application/json'
      res.body = JSON.generate(data)
    rescue => e
      res.status = 500
      res.content_type = 'application/json'
      res.body = JSON.generate({ error: e.message })
    end
  end
end
