require 'eventmachine'
require 'msgpack'
require 'concurrent'
require 'socket'
require_relative 'member'
require_relative 'state_manager'
require_relative 'logger'

module Swim
  class Protocol
    PROTOCOL_PERIOD = 1.0  # seconds
    PING_TIMEOUT = 0.5     # seconds
    SUSPECT_TIMEOUT = 5.0  # seconds
    SYNC_INTERVAL = 5.0    # seconds

    attr_reader :host, :port, :members, :incarnation, :service, :state_manager

    def initialize(host, port, seeds = [], service = nil)
      @host = host
      @port = port
      @members = Concurrent::Map.new
      @incarnation = 0
      @seeds = seeds
      @service = service
      @state_manager = StateManager.new
      
      # Add self as a member
      local_member = Member.new(@host, @port, @incarnation)
      @members[local_member.address] = local_member
      Logger.info("Initialized protocol node #{local_member.address}")

      setup_state_sync
    end

    def start
      if port_in_use?(@port)
        Logger.error("Port #{@port} is already in use")
        raise "Port #{@port} is already in use"
      end
      
      EM.run do
        setup_server
        setup_timers
        if @seeds.any?
          Logger.info("Joining cluster with seeds: #{@seeds.join(', ')}")
          join_cluster
        else
          Logger.info("Starting new cluster on #{@host}:#{@port}")
        end
      end
    end

    private

    def port_in_use?(port)
      begin
        server = TCPServer.new(@host, port)
        server.close
        false
      rescue Errno::EADDRINUSE
        true
      end
    end

    def setup_server
      EM.start_server(@host, @port, Connection) do |conn|
        conn.protocol = self
      end
    end

    def setup_timers
      EM.add_periodic_timer(PROTOCOL_PERIOD) { protocol_round }
      EM.add_periodic_timer(SUSPECT_TIMEOUT) { check_suspects }
      EM.add_periodic_timer(SYNC_INTERVAL) { sync_state }
    end

    def protocol_round
      return if @members.size <= 1
      
      member = select_random_member
      return unless member

      Logger.debug("Protocol round: pinging #{member.address}")
      ping_member(member)
    end

    def ping_member(member)
      message = {
        type: :ping,
        source: "#{@host}:#{@port}",
        incarnation: @incarnation
      }.to_msgpack

      EM.connect(member.host, member.port, Connection) do |conn|
        conn.protocol = self
        conn.send_data(message)
      end

      EM.add_timer(PING_TIMEOUT) do
        handle_ping_timeout(member) unless member.dead?
      end
    end

    def handle_ping_timeout(member)
      Logger.warn("Member #{member.address} timed out, marking as suspect")
      member.suspect!
      disseminate_updates([member])
    end

    def check_suspects
      now = Time.now.to_f
      @members.values.each do |member|
        if member.suspect? && (now - member.last_state_change_at) > SUSPECT_TIMEOUT
          Logger.warn("Member #{member.address} timed out, marking as dead")
          member.update(:dead, member.incarnation + 1)
        end
      end
    end

    def select_random_member
      alive_members = @members.values.reject { |m| m.dead? || m.address == "#{@host}:#{@port}" }
      alive_members.sample
    end

    def disseminate_updates(updates)
      message = {
        type: :update,
        source: "#{@host}:#{@port}",
        updates: updates.map { |m| [m.address, m.status, m.incarnation] }
      }.to_msgpack

      @members.values.each do |member|
        next if member.dead? || member.address == "#{@host}:#{@port}"
        
        host, port = member.address.split(':')
        Logger.debug("Sending message to #{member.address}")
        EM.connect(host, port.to_i, Connection) do |conn|
          conn.protocol = self
          conn.send_data(message)
        end
      end
    end

    def join_cluster
      @seeds.each do |seed|
        host, port = seed.split(':')
        message = {
          type: :join,
          source: "#{@host}:#{@port}",
          incarnation: @incarnation
        }.to_msgpack

        EM.connect(host, port.to_i, Connection) do |conn|
          conn.protocol = self
          conn.send_data(message)
        end
      end
    end

    def setup_state_sync
      Logger.debug("Setting up state synchronization")
      @state_manager.subscribe do |key, value, operation|
        broadcast_state_update(key, value, operation)
      end
    end

    def broadcast_state_update(key, value, operation)
      Logger.debug("Broadcasting state update: #{key} = #{value} (#{operation})")
      message = {
        type: :state_update,
        source: "#{@host}:#{@port}",
        updates: [[key, value, operation]]
      }.to_msgpack

      broadcast_to_members(message)
    end

    def broadcast_to_members(message)
      @members.values.each do |member|
        next if member.dead? || member.address == "#{@host}:#{@port}"
        
        host, port = member.address.split(':')
        Logger.debug("Sending message to #{member.address}")
        EM.connect(host, port.to_i, Connection) do |conn|
          conn.protocol = self
          conn.send_data(message)
        end
      end
    end

    def sync_state
      Logger.debug("Initiating state sync")
      snapshot = @state_manager.snapshot
      message = {
        type: :state_sync,
        source: "#{@host}:#{@port}",
        snapshot: snapshot
      }.to_msgpack

      broadcast_to_members(message)
    end
  end

  class Connection < EM::Connection
    attr_accessor :protocol

    def receive_data(data)
      message = MessagePack.unpack(data)
      handle_message(message)
    end

    private

    def handle_message(message)
      case message['type'].to_s
      when 'ping'
        handle_ping(message)
      when 'update'
        handle_update(message)
      when 'join'
        handle_join(message)
      when 'request'
        handle_request(message)
      when 'state_update'
        handle_state_update(message)
      when 'state_sync'
        handle_state_sync(message)
      end
    end

    def handle_ping(message)
      response = {
        type: :ack,
        source: message['source'],
        incarnation: protocol.incarnation
      }.to_msgpack
      
      send_data(response)
    end

    def handle_update(message)
      message['updates'].each do |address, status, incarnation|
        member = protocol.members[address]
        if member
          case status.to_s
          when Member::ALIVE.to_s
            member.alive!(incarnation)
          when Member::SUSPECT.to_s
            member.suspect!
          when Member::DEAD.to_s
            member.dead!
          end
        end
      end
    end

    def handle_join(message)
      host, port = message['source'].split(':')
      new_member = Member.new(host, port.to_i, message['incarnation'])
      protocol.members[new_member.address] = new_member
      
      # Send current membership list
      updates = protocol.members.values.map { |m| [m.address, m.status, m.incarnation] }
      response = {
        type: :update,
        source: "#{protocol.host}:#{protocol.port}",
        updates: updates
      }.to_msgpack
      
      send_data(response)
    end

    def handle_request(message)
      response = protocol.service.handle_request(
        message['path'],
        message['payload']
      )
      
      send_data(response.to_msgpack)
    end

    def handle_state_update(message)
      message['updates'].each do |key, value, operation|
        protocol.state_manager.apply(key, value, operation)
      end
    end

    def handle_state_sync(message)
      protocol.state_manager.sync(message['snapshot'])
    end
  end
end
