require 'webrick'
require 'json'

module Swim
  class HTTPServer
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
        status = {
          node: "#{@protocol.host}:#{@protocol.port}",
          alive_members: @protocol.alive_members.size,
          suspect_members: @protocol.suspect_members.size,
          dead_members: @protocol.dead_members.size,
          total_members: @protocol.members.size,
          uptime: Time.now.to_i - @start_time
        }
        json_response(res, status)
      end

      # GET /members - List all members and their status
      @server.mount_proc '/members' do |_, res|
        members = @protocol.members.map do |member|
          {
            address: member.address,
            status: member.status,
            last_response: member.last_response&.iso8601,
            last_state_change: member.last_state_change_at,
            incarnation: member.incarnation
          }
        end
        json_response(res, members)
      end

      # GET /metadata - Get node metadata
      @server.mount_proc '/metadata' do |_, res|
        metadata = {}
        @protocol.metadata.each_pair do |key, value|
          next if key.nil?
          namespace, k = key.split(':', 2)
          next if k.nil?

          metadata[namespace] ||= {}
          metadata[namespace][k] = value
        end
        json_response(res, { metadata: metadata })
      end

      # GET /state - Get distributed state
      @server.mount_proc '/state' do |_, res|
        state = {}
        @protocol.state_manager.instance_variable_get(:@state).each_pair do |namespace, data|
          next if namespace.nil? || !data
          state[namespace] = data.each_pair.each_with_object({}) do |(k, v), hash|
            next if k.nil?
            hash[k] = v
          end
        end
        json_response(res, { 
          state: state,
          version: @protocol.state_manager.version
        })
      end
    end

    def json_response(res, data)
      res.content_type = 'application/json'
      res.status = 200
      res.body = JSON.generate(data)
    rescue => e
      res.status = 500
      res.content_type = 'application/json'
      res.body = JSON.generate({ error: e.message })
    end
  end
end
