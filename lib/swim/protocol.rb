require 'socket'
require 'concurrent'
require 'msgpack'
require_relative 'member'
require_relative 'logger'
require_relative 'message'
require_relative 'codec'
require_relative 'network'
require_relative 'directory'

module Swim
  class Protocol
    PROTOCOL_PERIOD = 1.0  # seconds
    PING_TIMEOUT = 0.5     # seconds
    PING_REQUEST_TIMEOUT = 0.5  # seconds
    SYNC_INTERVAL = 10.0  # seconds

    attr_reader :host, :port, :directory

    def initialize(host, port, seeds = [])
      @host = host
      @port = port.to_i
      @seeds = seeds
      @running = false
      @self_addr = "#{@host}:#{@port}"

      # Initialize components
      @directory = Directory.new
      @network = Network.new(host, port)

      # Setup event handlers
      setup_event_handlers

      # Add self as a member and set as current node
      self_member = Member.new(@host, @port)
      @directory.add_member(self_member)
      @directory.current_node = self_member
    end

    def start
      return if @running
      @running = true
      
      @network.start
      setup_periodic_tasks
      join_cluster
    end

    def stop
      return unless @running
      @running = false
      
      @network.stop
      @ping_task&.shutdown
      @check_task&.shutdown
    end

    def on_member_change(&block)
      @directory.subscribe(block)
    end

    def alive_members
      @directory.alive_peers
    end

    def suspect_members
      @directory.suspicious_peers
    end

    def dead_members
      @directory.failed_peers
    end

    def members
      @directory.all_members
    end

    # Network event handlers
    def message_received(message, remote_addr)
      case message.type
      when :join then handle_join(message.sender)
      when :ping then handle_ping(message.sender)
      when :ack then handle_ack(message.sender)
      when :ping_req then handle_ping_req(message.sender, message.target, message.data)
      when :ping_ack then handle_ping_ack(message.sender, message.target)
      when :suspect then handle_suspect(message.sender, message.target)
      when :alive then handle_alive(message.sender, message.target, message.data[:incarnation])
      when :dead then handle_dead(message.sender, message.target)
      end
    end

    def message_sent(message, target_host, target_port, bytes_sent)
      Logger.debug("Message sent to #{target_host}:#{target_port} (#{bytes_sent} bytes)")
    end

    def send_error(error, message, target_host, target_port)
      Logger.error("Failed to send message to #{target_host}:#{target_port}: #{error.message}")
    end

    def receive_error(error)
      Logger.error("Error receiving message: #{error.message}")
    end

    def decode_error(error, data, sender)
      Logger.error("Failed to decode message from #{sender}: #{error.message}")
    end

    private

    def setup_event_handlers
      # Network events
      @network.subscribe(self)

      # Directory events
      @directory.on(:member_joined) { |member| Logger.info("Member joined: #{member.address}") }
      @directory.on(:member_left) { |member| Logger.info("Member left: #{member.address}") }
      @directory.on(:member_suspected) { |member| handle_member_suspected(member) }
      @directory.on(:member_failed) { |member| handle_member_failed(member) }
      @directory.on(:member_recovered) { |member| handle_member_recovered(member) }
    end

    def handle_member_suspected(member)
      Logger.info("Member suspected: #{member.address}")
      indirect_ping(member)
      broadcast_message(Message.suspect(@self_addr, member.address))
    end

    def handle_member_failed(member)
      Logger.info("Member failed: #{member.address}")
      broadcast_message(Message.dead(@self_addr, member.address))
    end

    def handle_member_recovered(member)
      Logger.info("Member recovered: #{member.address}")
      broadcast_message(Message.alive(@self_addr, member.address, member.incarnation))
    end

    def setup_periodic_tasks
      @ping_task = Concurrent::TimerTask.new(execution_interval: PROTOCOL_PERIOD, run_now: true) do
        ping_random_member
      end
      @check_task = Concurrent::TimerTask.new(execution_interval: PROTOCOL_PERIOD, run_now: true) do
        check_members
      end

      [@ping_task, @check_task].each(&:execute)
    end

    def broadcast_message(message)
      targets = @directory.alive_peers.map(&:address)
      @network.broadcast_message(message, targets)
    end

    def send_message(host, port, message)
      @network.send_message(message, host, port)
    end

    def check_members
      @directory.peers.each do |member|
        if member.pending_ping? && Time.now - member.last_response > PING_TIMEOUT
          if member.alive?
            @directory.update_member_status(member, :suspect)
          elsif member.suspect?
            @directory.update_member_status(member, :dead)
          end
        end
      end
    end

    def handle_join(sender_addr)
      return if sender_addr == @self_addr
      
      host, port = sender_addr.split(':')
      member = @directory.get_member(sender_addr)

      unless member
        # broadcast join message to let other nodes know
        broadcast_message(Message.join(sender_addr))

        @directory.add_member(Member.new(host, port))
        member = Member.new(host, port)
      end
      
      @directory.update_member_status(member, :alive)
      # Send ack back to the joining member
      send_message(host, port.to_i, Message.ack(@self_addr, sender_addr))   
    end

    def handle_ping(sender_addr)
      handle_join(sender_addr)
    end

    def handle_ack(sender_addr)
      unless (member = @directory.get_member(sender_addr))
        host, port = sender_addr.split(':')
        member = Member.new(host, port)
        @directory.add_member(member)
      end
      @directory.update_member_status(member, :alive)
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
      return unless (member = @directory.get_member(target_addr))
      @directory.update_member_status(member, :alive)
      member.clear_pending_ping
    end

    def handle_suspect(sender_addr, target_addr)
      return unless (member = @directory.get_member(target_addr))
      return if member.address == @self_addr
      
      @directory.update_member_status(member, :suspect)
    end

    def handle_alive(sender_addr, target_addr, incarnation)
      unless (member = @directory.get_member(target_addr))
        host, port = target_addr.split(':')
        member = Member.new(host, port, incarnation)
        @directory.add_member(member)
      end
      return if member.address == @self_addr
      
      if incarnation > member.incarnation
        @directory.update_member_status(member, :alive)
        member.incarnation = incarnation
      end
    end

    def handle_dead(sender_addr, target_addr)
      return unless (member = @directory.get_member(target_addr))
      return if member.address == @self_addr
      
      @directory.update_member_status(member, :dead)
    end

    def indirect_ping(member)
      k = 3 # Number of random members to try
      alive_peers = @directory.alive_peers.reject { |m| m.address == member.address }
      return if alive_peers.empty?

      targets = alive_peers.sample([k, alive_peers.size].min)
      targets.each do |target|
        send_message(target.host, target.port, Message.ping_req(@self_addr, member.address))
      end
    end

    def join_cluster
      return if @seeds.empty?
      
      @seeds.each do |seed|
        next if seed == @self_addr
        host, port = seed.split(':')
        send_message(host, port.to_i, Message.join(@self_addr))
      end
    end

    def ping_random_member
      alive_peers = @directory.alive_peers
      return if alive_peers.empty?

      member = alive_peers.sample
      member.pending_ping = Time.now
      send_message(member.host, member.port, Message.ping(@self_addr, member.address))
    end

  end
end
