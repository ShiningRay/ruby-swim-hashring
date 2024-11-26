require 'socket'
require 'concurrent'
require 'msgpack'
require_relative 'member'
require_relative 'state_manager'
require_relative 'logger'
require_relative 'message'
require_relative 'codec'
require_relative 'network'

module Swim
  class Protocol
    PROTOCOL_PERIOD = 1.0  # seconds
    PING_TIMEOUT = 0.5     # seconds
    PING_REQUEST_TIMEOUT = 0.5  # seconds
    SYNC_INTERVAL = 10.0  # seconds

    attr_reader :members, :state_manager, :host, :port

    # Creates a new instance of the protocol
    #
    # @param host [String] The host address to bind to
    # @param port [Integer] The port number to bind to
    # @param seeds [Array<String>] An array of seed hosts in the format ["host:port", ...]
    # @param initial_metadata [Hash] A hash of initial metadata for the node
    # @param test_mode [Boolean] Whether to enable test mode (default: false)
    def initialize(host, port, seeds = [], initial_metadata = {})
      @host = host
      @port = port.to_i
      @seeds = seeds
      @members = Concurrent::Map.new
      @callbacks = []
      @running = false
      @metadata = Concurrent::Map.new

      # Add self as a member
      @self_addr = "#{@host}:#{@port}"
      @members[@self_addr] = Member.new(@host, @port)

      # Initialize network
      @network = Network.new(host, port)
      @network.subscribe(self)
      
      # Initialize metadata
      initial_metadata.each do |key, value|
        set_metadata(key, value)
      end
    end

    # Starts the protocol. This method is idempotent, meaning it only starts the
    # protocol once. If the protocol is already running, this method does
    # nothing.
    #
    # This method starts three threads:
    #
    # 1. The protocol loop thread, which sends SWIM protocol messages to other
    #    nodes.
    # 2. The receive loop thread, which listens for incoming SWIM protocol
    #    messages.
    # 3. The state synchronization thread, which periodically sends the node's
    #    state to other nodes.
    #
    # If the node is configured with seed nodes, this method will send a join
    # message to the seed nodes.
    def start
      return if @running
      @running = true
      
      @network.start
      setup_periodic_tasks
      join_cluster
    end

    # Stops the SWIM protocol.
    #
    # This method is idempotent and only performs actions if the protocol is running.
    # It closes the socket, waits for the protocol, receive, and sync threads to finish,
    # and logs that the protocol has stopped.
    def stop
      return unless @running
      @running = false
      
      @network.stop
      @ping_task&.shutdown
      @check_task&.shutdown
      @sync_task&.shutdown
    end

    def get_metadata(key, namespace = 'default')
      return nil if key.nil? || namespace.nil?
      @state_manager.get(key, namespace)
    end

    def set_metadata(key, value, namespace = 'default')
      return false if key.nil? || namespace.nil?
      @state_manager.set(key, value, namespace)
      true
    end

    def delete_metadata(key, namespace = 'default')
      return false if key.nil? || namespace.nil?
      @state_manager.delete(key, namespace)
      true
    end

    def on_metadata_change(&block)
      @metadata_callbacks << block
    end

    def on_member_change(&block)
      @callbacks << block
    end

    def alive_members
      @members.values.select { | m| m.alive? }.map(&:address)
    end

    def suspect_members
      @members.values.select { | m| m.suspect? }.map(&:address)
    end

    def dead_members
      @members.values.select { | m| m.dead? }.map(&:address)
    end

    def metadata
      @state_manager.get_namespace('default')
    end

    def merge_metadata(metadata)
      return unless metadata.is_a?(Hash)
      
      metadata.each do |namespace, data|
        next unless namespace.is_a?(String) && data.is_a?(Hash)
        data.each do |key, value|
          next if key.nil?
          set_metadata(key, value, namespace)
        end
      end
    end

    def members
      @members.values
    end

    def add_member(host, port)
      addr = "#{host}:#{port}"
      return if addr == @self_addr  # Don't add self again
      
      unless @members[addr]
        member = Member.new(host, port)
        @members[addr] = member
        Logger.info("Added new member: #{addr}")
      end
    end

    def initialize_members(seeds)
      seeds.each do |seed|
        next if seed == @self_addr
        host, port = seed.split(':')
        add_member(host, port.to_i)
      end
    end

    def on_message_received(message, remote_addr)
      case message.type
      when :join
        handle_join(message.sender)
      when :ping
        handle_ping(message.sender)
      when :ack
        handle_ack(message.sender)
      when :ping_req
        handle_ping_req(message.sender, message.target, message.data)
      when :ping_ack
        handle_ping_ack(message.sender, message.target)
      when :suspect
        handle_suspect(message.sender, message.target)
      when :alive
        handle_alive(message.sender, message.target, message.data[:incarnation])
      when :dead
        handle_dead(message.sender, message.target)
      when :metadata
        handle_metadata(message.sender, message.data)
      else
        Logger.warn("Unknown message type: #{message.type}")
      end
    end

    def on_message_sent(message, target_host, target_port, bytes_sent)
      Logger.debug("Sent #{message.type} to #{target_host}:#{target_port} (#{bytes_sent} bytes)")
    end

    def on_send_error(error, message, target_host, target_port)
      Logger.error("Failed to send #{message.type} to #{target_host}:#{target_port}: #{error.message}")
      handle_member_unreachable("#{target_host}:#{target_port}")
    end

    def on_receive_error(error)
      Logger.error("Error in receive loop: #{error.message}")
      Logger.debug(error.backtrace.join("\n"))
    end

    def on_decode_error(error, data, sender)
      Logger.error("Failed to decode message from #{sender[3]}:#{sender[1]}: #{error.message}")
    end

    private

    def notify_member_change(addr, old_status, new_status)
      @callbacks.each do |callback|
        callback.call(addr, old_status, new_status)
      end
    end

    def setup_periodic_tasks
      @ping_task = Concurrent::TimerTask.new(execution_interval: PROTOCOL_PERIOD, run_now: true) do
        ping_random_member
      end
      @check_task = Concurrent::TimerTask.new(execution_interval: PROTOCOL_PERIOD, run_now: true) do
        check_members
      end
      @sync_task = Concurrent::TimerTask.new(execution_interval: SYNC_INTERVAL, run_now: true) do
        setup_state_sync
      end
    end

    def broadcast_message(message)
      targets = @members.select { |addr, member| 
        addr != @self_addr && !member.dead?
      }.keys
      @network.broadcast_message(message, targets)
    end

    def send_message(host, port, message)
      @network.send_message(message, host, port)
    end

    def check_members
      @members.each_pair do |addr, member|
        next if addr == @self_addr
        
        if member.pending_ping? && Time.now - member.last_response > PING_TIMEOUT
          if member.alive?
            old_status = member.status
            member.mark_suspect
            notify_member_change(addr, old_status, :suspect)
            indirect_ping(member)
          elsif member.suspect?
            old_status = member.status
            member.mark_dead
            notify_member_change(addr, old_status, :dead)
            @members.delete(addr)
            Logger.info("Removed failed member: #{addr}")
          end
        end
      end
    end

    def handle_join(sender_addr)
      return if sender_addr == @self_addr
      
      host, port = sender_addr.split(':')
      member = @members[sender_addr] || Member.new(host, port)
      @members[sender_addr] = member
      member.mark_alive
      
      # Send ack back to the joining member
      send_message(host, port.to_i, Message.ack(@self_addr, sender_addr))
      
      # Broadcast to other members about the new join
      broadcast_message(Message.alive(@self_addr, sender_addr))
      
      Logger.info("Member joined: #{sender_addr}")
    end

    def handle_ping(sender_addr)
      return if sender_addr == @self_addr
      
      host, port = sender_addr.split(':')
      send_message(host, port.to_i, Message.ack(@self_addr, sender_addr))
    end

    def handle_ack(sender_addr)
      return unless @members[sender_addr]
      member = @members[sender_addr]
      member.mark_alive
      member.clear_pending_ping
    end

    def handle_ping_req(sender_addr, target_addr, data)
      return if target_addr == @self_addr
      
      target_host, target_port = target_addr.split(':')
      if send_message(target_host, target_port.to_i, Message.ping(@self_addr, target_addr))
        sender_host, sender_port = sender_addr.split(':')
        send_message(sender_host, sender_port.to_i, Message.ping_ack(@self_addr, target_addr, data))
      end
    end

    def handle_ping_ack(sender_addr, target_addr)
      return unless @members[target_addr]
      member = @members[target_addr]
      member.mark_alive
      member.clear_pending_ping
    end

    def handle_suspect(sender_addr, target_addr)
      return if target_addr == @self_addr
      return unless @members[target_addr]
      
      member = @members[target_addr]
      if member.alive?
        old_status = member.status
        member.mark_suspect
        notify_member_change(target_addr, old_status, :suspect)
        
        # Try indirect ping
        indirect_ping(member)
      elsif member.suspect?
        old_status = member.status
        member.mark_dead
        notify_member_change(target_addr, old_status, :dead)
        @members.delete(target_addr)
      end
    end

    def handle_alive(sender_addr, target_addr, incarnation)
      return if target_addr == @self_addr
      return unless @members[target_addr]
      
      member = @members[target_addr]
      if member.suspect?
        old_status = member.status
        member.mark_alive
        notify_member_change(target_addr, old_status, :alive)
      end
    end

    def handle_dead(sender_addr, target_addr)
      return if target_addr == @self_addr
      return unless @members[target_addr]
      
      member = @members[target_addr]
      old_status = member.status
      member.mark_dead
      notify_member_change(target_addr, old_status, :dead)
      @members.delete(target_addr)
    end

    def handle_metadata(sender_addr, metadata)
      return unless @members[sender_addr]
      member = @members[sender_addr]
      metadata.each do |key, value|
        member.set_metadata(key, value)
      end
    end

    def indirect_ping(member)
      # Select k random members to help with the indirect ping
      k = 3
      helpers = @members.select { |addr, m| 
        addr != @self_addr && 
        addr != member.address && 
        m.alive?
      }.to_a.sample(k)

      helpers.each do |addr, helper|
        host, port = addr.split(':')
        message = Message.ping_req(@self_addr, member.address, { helper: addr })
        send_message(host, port.to_i, message)
      end
    end

    def setup_state_sync
      return if @members.empty?
      
      # 随机选择一个活跃的成员进行状态同步
      alive_addrs = @members.each_pair.select { |_, m| m.alive? }.map(&:first)
      return if alive_addrs.empty?
      
      target_addr = alive_addrs.sample
      host, port = target_addr.split(':')
      
      send_message(host, port.to_i, Message.state_sync(@self_addr, @state_manager.snapshot))
    rescue => e
      Logger.error("Error in state sync: #{e.message}")
      Logger.debug(e.backtrace.join("\n"))
    end

    def broadcast_state_update(key, value, operation)
      return if @members.empty?
      
      broadcast_message(Message.state_update(@self_addr, [[key, value, operation]]))
    end

    def join_cluster
      Logger.info("Joining cluster with seeds: #{@seeds.join(', ')}")
      message = Message.join(@self_addr)
      @seeds.each do |seed|
        next if seed == @self_addr
        host, port = seed.split(':')
        Logger.info("Sending join request to seed: #{seed}")
        send_message(host, port.to_i, message)
      end
    end

    def ping_random_member
      return if @members.empty?
      
      # 随机选择一个活跃的成员
      alive_members = @members.values.select(&:alive?)
      return if alive_members.empty?

      member = alive_members.sample
      return if member.address == @self_addr  # 不要 ping 自己
      return if member.pending_ping?  # Skip if already waiting for response

      message = Message.ping(@self_addr, member.address)
      if send_message(member.host, member.port, message)
        member.set_pending_ping
      end
    end
  end
end
