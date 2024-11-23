require 'socket'
require 'concurrent'
require 'msgpack'
require_relative 'member'
require_relative 'state_manager'
require_relative 'logger'

module Swim
  class Protocol
    PROTOCOL_PERIOD = 1.0  # seconds
    PING_TIMEOUT = 0.5     # seconds
    PING_REQUEST_TIMEOUT = 0.5  # seconds

    attr_reader :members, :state_manager

    def initialize(host, port, seeds, service)
      @host = host
      @port = port
      @service = service
      @seeds = seeds
      @state_manager = StateManager.new
      @members = {}
      @running = false
      @socket = UDPSocket.new
      @socket.bind(@host, @port)

      setup_state_sync
      initialize_members(seeds)
    end

    def start
      return if @running
      @running = true
      Logger.info("Starting SWIM protocol on #{@host}:#{@port}")
      
      @protocol_thread = Thread.new { run_protocol_loop }
      @receive_thread = Thread.new { run_receive_loop }

      # Send join message to seeds
      join_cluster if @seeds && !@seeds.empty?
    end

    def stop
      return unless @running
      
      # Broadcast leave message
      Logger.info("Broadcasting leave message")
      message = {
        'type' => 'leave',
        'source' => "#{@host}:#{@port}",
        'timestamp' => Time.now.to_f
      }
      broadcast_message(message)
      
      # Wait a bit for the message to be sent
      sleep(0.1)
      
      @running = false
      
      # Close socket to interrupt receive_thread
      @socket.close rescue nil
      
      # Give threads a chance to exit gracefully
      begin
        Timeout.timeout(2) do
          @protocol_thread&.join
          @receive_thread&.join
        end
      rescue Timeout::Error
        # Force kill threads if they don't exit in time
        @protocol_thread&.kill
        @receive_thread&.kill
        Logger.warn("Had to force kill protocol threads")
      end
      
      Logger.info("Protocol stopped")
    end

    private

    def run_protocol_loop
      while @running
        begin
          ping_random_member
          sleep(PROTOCOL_PERIOD)
        rescue => e
          break unless @running
          Logger.error("Error in protocol loop: #{e.message}\n#{e.backtrace.join("\n")}")
        end
      end
    rescue => e
      Logger.error("Protocol loop terminated: #{e.message}\n#{e.backtrace.join("\n")}")
    end

    def run_receive_loop
      while @running
        begin
          # Use timeout to allow checking @running periodically
          ready = IO.select([@socket], nil, nil, 1)
          next unless ready
          
          data, addr = @socket.recvfrom(65535)
          next unless @running # Check again after potentially long receive
          
          source_host = addr[3]
          source_port = addr[1]
          message = MessagePack.unpack(data)
          handle_message(message)
        rescue IOError, Errno::EBADF
          break unless @running
        rescue => e
          break unless @running
          Logger.error("Error in receive loop: #{e.message}\n#{e.backtrace.join("\n")}")
        end
      end
    rescue => e
      Logger.error("Receive loop terminated: #{e.message}\n#{e.backtrace.join("\n")}")
    end

    def send_message(host, port, message)
      begin
        data = message.to_msgpack
        @socket.send(data, 0, host, port)
      rescue => e
        Logger.error("Failed to send message to #{host}:#{port}: #{e.message}")
      end
    end

    def broadcast_message(message)
      @members.each_value do |member|
        next if member.address == "#{@host}:#{@port}"
        host, port = member.address.split(':')
        send_message(host, port.to_i, message)
      end
    end

    def ping_random_member
      return if @members.empty?
      
      member = @members.values.reject { |m| m.address == "#{@host}:#{@port}" }.sample
      return unless member

      host, port = member.address.split(':')
      message = {
        'type' => 'ping',
        'source' => "#{@host}:#{@port}",
        'timestamp' => Time.now.to_f
      }

      send_message(host, port.to_i, message)
      
      # Wait for response with timeout
      start_time = Time.now
      while Time.now - start_time < PING_TIMEOUT
        sleep(0.1)
        return if member.last_response && member.last_response > start_time
      end

      # If no response, mark as suspicious and try indirect ping
      member.mark_suspicious
      indirect_ping(member)
    end

    def indirect_ping(target_member)
      k = 3 # number of indirect ping members
      indirect_members = @members.values
                                .reject { |m| m.address == target_member.address || m.address == "#{@host}:#{@port}" }
                                .sample(k)

      return if indirect_members.empty?

      message = {
        'type' => 'ping_req',
        'source' => "#{@host}:#{@port}",
        'target' => target_member.address,
        'timestamp' => Time.now.to_f
      }

      indirect_members.each do |member|
        host, port = member.address.split(':')
        send_message(host, port.to_i, message)
      end

      # Wait for indirect ping response
      start_time = Time.now
      while Time.now - start_time < PING_REQUEST_TIMEOUT
        sleep(0.1)
        return if target_member.last_response && target_member.last_response > start_time
      end

      target_member.mark_failed
      remove_member(target_member)
    end

    def handle_message(message)
      Logger.debug("Received message: #{message}")
      
      case message['type'].to_s
      when 'ping'
        handle_ping(message)
      when 'ack'
        handle_ack(message)
      when 'ping_req'
        handle_ping_req(message)
      when 'join'
        handle_join(message)
      when 'leave'
        handle_leave(message)
      when 'update'
        handle_update(message)
      when 'members'
        handle_members(message)
      when 'member_joined'
        handle_member_joined(message)
      when 'state_sync'
        handle_state_sync(message)
      when 'state_update'
        handle_state_update(message)
      else
        Logger.warn("Unknown message type: #{message['type']}")
      end
    end

    def handle_join(message)
      source = message['source']
      return if source == "#{@host}:#{@port}"

      # Add the new member
      unless @members[source]
        host, port = source.split(':')
        member = Member.new(host, port.to_i)
        @members[source] = member
        Logger.info("New member joined: #{source}")
        Logger.debug { "Current member list: #{@members.keys.join(', ')}" }

        # Send current member list to the new member
        response = {
          'type' => 'members',
          'source' => "#{@host}:#{@port}",
          'members' => @members.keys,
          'timestamp' => Time.now.to_f
        }
        host, port = source.split(':')
        send_message(host, port.to_i, response)

        # Broadcast new member to all existing members
        broadcast = {
          'type' => 'member_joined',
          'source' => "#{@host}:#{@port}",
          'member' => source,
          'timestamp' => Time.now.to_f
        }
        broadcast_message(broadcast)
      end
    end

    def handle_members(message)
      return if message['source'] == "#{@host}:#{@port}"
      
      Logger.info("Received member list from #{message['source']}")
      message['members'].each do |member_addr|
        next if member_addr == "#{@host}:#{@port}" || @members[member_addr]
        
        host, port = member_addr.split(':')
        member = Member.new(host, port.to_i)
        @members[member_addr] = member
        Logger.debug { "Added member: #{member_addr}" }
      end
      Logger.debug { "Updated member list: #{@members.keys.join(', ')}" }
    end

    def handle_member_joined(message)
      return if message['source'] == "#{@host}:#{@port}"
      
      member_addr = message['member']
      unless @members[member_addr]
        host, port = member_addr.split(':')
        member = Member.new(host, port.to_i)
        @members[member_addr] = member
        Logger.info("Member joined (broadcast): #{member_addr}")
        Logger.debug { "Current member list: #{@members.keys.join(', ')}" }
      end
    end

    def handle_ping(message)
      response = {
        'type' => 'ack',
        'source' => "#{@host}:#{@port}",
        'in_response_to' => message['timestamp']
      }
      
      source_host, source_port = message['source'].split(':')
      send_message(source_host, source_port.to_i, response)
    end

    def handle_ack(message)
      source = message['source']
      member = @members[source]
      return unless member

      member.mark_alive
      member.last_response = Time.now
    end

    def handle_ping_req(message)
      target_host, target_port = message['target'].split(':')
      ping_message = {
        'type' => 'ping',
        'source' => "#{@host}:#{@port}",
        'timestamp' => Time.now.to_f
      }

      send_message(target_host, target_port.to_i, ping_message)
    end

    def handle_leave(message)
      source = message['source']
      return if source == "#{@host}:#{@port}"

      if @members[source]
        Logger.info("Member left: #{source}")
        @members.delete(source)
        Logger.debug { "Current member list: #{@members.keys.join(', ')}" }
      end
    end

    def handle_update(message)
      source = message['source']
      member = @members[source]
      return unless member

      member.status = message['status']
      Logger.info("Member #{source} status updated to #{message['status']}")
    end

    def handle_state_sync(message)
      if message['state'] && message['source'] != "#{@host}:#{@port}"
        @state_manager.merge(message['state'])
        Logger.info("State synchronized with #{message['source']}")
      else
        Logger.warn("Failed to apply state snapshot from #{message['source']}")
      end
    end

    def handle_state_update(message)
      message['updates'].each do |key, value, operation|
        @state_manager.merge_update([[key, value, operation]])
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
        'type' => 'state_update',
        'source' => "#{@host}:#{@port}",
        'updates' => [[key, value, operation]]
      }
      broadcast_message(message)
    end

    def initialize_members(seeds)
      seeds.each do |seed|
        next if seed == "#{@host}:#{@port}"
        host, port = seed.split(':')
        @members[seed] = Member.new(host, port.to_i)
      end
    end

    def remove_member(member)
      @members.delete(member.address)
      Logger.info("Removed failed member: #{member.address}")
      broadcast_message({
        'type' => 'update',
        'source' => member.address,
        'status' => :failed
      })
    end

    def join_cluster
      Logger.info("Joining cluster with seeds: #{@seeds.join(', ')}")
      message = {
        'type' => 'join',
        'source' => "#{@host}:#{@port}",
        'timestamp' => Time.now.to_f
      }

      @seeds.each do |seed|
        next if seed == "#{@host}:#{@port}"
        host, port = seed.split(':')
        send_message(host, port.to_i, message)
      end
    end
  end
end
