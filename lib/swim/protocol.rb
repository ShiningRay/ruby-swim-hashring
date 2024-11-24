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
    SYNC_INTERVAL = 10.0  # seconds

    attr_reader :members, :state_manager, :host, :port

    def initialize(host, port, seeds = [], initial_metadata = {})
      @host = host
      @port = port
      @seeds = Array(seeds)
      @members = Concurrent::Map.new
      @state_manager = StateManager.new
      @running = false
      @callbacks = Concurrent::Array.new
      @metadata_callbacks = Concurrent::Array.new
      
      # 初始化 socket
      @socket = UDPSocket.new
      @socket.bind(host, port)
      
      # 初始化元数据
      initial_metadata.each do |namespace, data|
        data.each do |key, value|
          set_metadata(key, value, namespace)
        end
      end
      
      initialize_members(seeds)
      setup_state_sync
    end

    def start
      return if @running
      @running = true
      
      # 启动协议循环
      @protocol_thread = Thread.new do
        run_protocol_loop
      end
      
      # 启动接收循环
      @receive_thread = Thread.new do
        run_receive_loop
      end
      
      # Send join message to seeds
      join_cluster if @seeds && !@seeds.empty?
    end

    def stop
      return unless @running
      @running = false
      
      # 关闭 socket
      @socket.close rescue nil
      
      # 等待线程结束
      [@protocol_thread, @receive_thread, @sync_thread].each do |thread|
        thread&.join(1) # 等待最多1秒
        thread&.kill if thread&.alive?
      end
      
      @protocol_thread = nil
      @receive_thread = nil
      @sync_thread = nil
      Logger.info("Stopped SWIM protocol on #{@host}:#{@port}")
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
      @members.select { |_, m| m.alive? }.keys
    end

    def suspect_members
      @members.select { |_, m| m.suspect? }.keys
    end

    def dead_members
      @members.select { |_, m| m.dead? }.keys
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

    private

    def notify_member_change(member_addr, old_status, new_status)
      return unless @callbacks
      @callbacks.each do |callback|
        callback.call(member_addr, old_status, new_status)
      end
    end

    def run_receive_loop
      buffer = []
      while @running
        begin
          ready = IO.select([@socket], nil, nil, 1)
          next unless ready
          
          data, addr = @socket.recvfrom(65535)
          message = MessagePack.unpack(data)
          handle_message(message)
        rescue IOError, Errno::EBADF => e
          break if !@running  # 正常退出
          Logger.error("Socket closed unexpectedly: #{e.message}")
          break
        rescue => e
          Logger.error("Error in receive loop: #{e.message}")
          Logger.debug(e.backtrace.join("\n"))
        end
      end
    rescue => e
      Logger.error("Fatal error in receive loop: #{e.message}")
      Logger.debug(e.backtrace.join("\n"))
    end

    def run_protocol_loop
      while @running
        begin
          check_members
          ping_random_member
          sleep(1)
        rescue => e
          Logger.error("Error in protocol loop: #{e.message}")
          Logger.debug(e.backtrace.join("\n"))
        end
      end
    rescue => e
      Logger.error("Fatal error in protocol loop: #{e.message}")
      Logger.debug(e.backtrace.join("\n"))
    end

    def check_members
      @members.each_pair do |addr, member|
        next if addr == "#{@host}:#{@port}"  # 跳过自己
        
        if member.check_timeouts
          # 如果返回 true，表示成员应该被移除
          @members.delete(addr)
          Logger.info("Removed failed member: #{addr}")
        end
      end
    end

    def send_message(host, port, message)
      begin
        socket = TCPSocket.new(host, port)
        socket.write(MessagePack.pack(message))
        socket.close
      rescue Errno::ECONNREFUSED
        Logger.debug("Connection refused by #{host}:#{port}")
        handle_member_unreachable("#{host}:#{port}")
      rescue => e
        Logger.error("Error sending message to #{host}:#{port}: #{e.message}")
        Logger.debug(e.backtrace.join("\n"))
        handle_member_unreachable("#{host}:#{port}")
      ensure
        socket&.close
      end
    end

    def handle_member_unreachable(addr)
      return unless addr && @members[addr]
      
      member = @members[addr]
      old_status = member.status
      
      case member.status
      when :alive
        member.status = :suspect
        notify_member_change(addr, old_status, :suspect)
      when :suspect
        member.status = :dead
        notify_member_change(addr, old_status, :dead)
      end
    end

    def broadcast_message(message)
      @members.each_pair do |addr, member|
        next if member.dead?
        host, port = addr.split(':')
        send_message(host, port.to_i, message)
      end
    end

    def setup_state_sync
      @sync_thread = Thread.new do
        loop do
          begin
            sleep(SYNC_INTERVAL)
            next if @members.empty?
            
            # 随机选择一个活跃的成员进行状态同步
            alive_addrs = @members.each_pair.select { |_, m| m.alive? }.map(&:first)
            next if alive_addrs.empty?
            
            target_addr = alive_addrs.sample
            host, port = target_addr.split(':')
            
            send_message(host, port.to_i, {
              'type' => 'state_sync',
              'source' => "#{@host}:#{@port}",
              'state' => @state_manager.snapshot
            })
          rescue => e
            Logger.error("Error in state sync: #{e.message}")
            Logger.debug(e.backtrace.join("\n"))
          end
        end
      end
    end

    def broadcast_state_update(key, value, operation)
      return if @members.empty?
      
      broadcast_message({
        'type' => 'state_update',
        'source' => "#{@host}:#{@port}",
        'updates' => [[key, value, operation]]
      })
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

    def ping_random_member
      return if @members.empty?
      
      # 随机选择一个活跃的成员
      alive_members = @members.values.select(&:alive?)
      return if alive_members.empty?

      member = alive_members.sample
      return if member.address == "#{@host}:#{@port}"  # 不要 ping 自己

      message = {
        'type' => 'ping',
        'source' => "#{@host}:#{@port}",
        'timestamp' => Time.now.to_f
      }

      if send_message(member.host, member.port, message)
        member.pending_ping = Time.now
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
      member.pending_ping = nil
    end

    def indirect_ping(target_member)
      # 选择 3 个其他成员发送间接 ping
      other_members = @members.values.reject { |m| 
        m.address == target_member.address || 
        m.address == "#{@host}:#{@port}" || 
        !m.alive?
      }

      k = [3, other_members.size].min
      return if k == 0

      ping_req_message = {
        'type' => 'ping_req',
        'source' => "#{@host}:#{@port}",
        'target' => target_member.address,
        'timestamp' => Time.now.to_f
      }

      other_members.sample(k).each do |member|
        send_message(member.host, member.port, ping_req_message)
      end
    end

    def handle_message(message)
      return unless message.is_a?(Hash) && message['type']
      Logger.debug("Received message: #{message}")
      
      case message['type']
      when 'join'
        handle_join(message)
      when 'members'
        handle_members(message)
      when 'member_joined'
        handle_member_joined(message)
      when 'ping'
        handle_ping(message)
      when 'ack'
        handle_ack(message)
      when 'ping_req'
        handle_ping_req(message)
      when 'leave'
        handle_leave(message)
      when 'update'
        handle_update(message)
      when 'state_sync'
        handle_state_sync(message)
      when 'state_update'
        handle_state_update(message)
      else
        Logger.warn("Unknown message type: #{message['type']}")
      end
    end

    def handle_join(message)
      return unless message && message['source']
      source = message['source']
      return if source == "#{@host}:#{@port}"

      host, port = source.split(':')
      return unless host && port

      member = Member.new(host, port.to_i)
      @members[source] = member
      Logger.info("Member joined: #{source}")
      Logger.debug { "Current member list: #{@members.keys.join(', ')}" }

      # 广播新成员加入的消息
      broadcast_message({
        'type' => 'member_joined',
        'source' => "#{@host}:#{@port}",
        'member' => source
      })

      # 发送当前成员列表给新加入的节点
      send_message(host, port.to_i, {
        'type' => 'members',
        'source' => "#{@host}:#{@port}",
        'members' => @members.keys
      })
    end

    def handle_members(message)
      return unless message && message['members'].is_a?(Array)
      
      message['members'].each do |member_addr|
        next unless member_addr.is_a?(String)
        next if member_addr == "#{@host}:#{@port}"
        next if @members[member_addr]

        begin
          host, port = member_addr.split(':')
          next unless host && port && port.to_i.positive?

          member = Member.new(host, port.to_i)
          @members[member_addr] = member
          Logger.debug("Added member from list: #{member_addr}")
        rescue => e
          Logger.error("Error adding member #{member_addr}: #{e.message}")
          Logger.debug(e.backtrace.join("\n"))
        end
      end
      
      Logger.debug { "Current member list: #{@members.keys.join(', ')}" }
    end

    def handle_member_joined(message)
      return unless message && message['source'] && message['member']
      return if message['source'] == "#{@host}:#{@port}"
      
      member_addr = message['member']
      return if @members[member_addr]
      return if member_addr == "#{@host}:#{@port}"

      begin
        host, port = member_addr.split(':')
        return unless host && port && port.to_i.positive?

        member = Member.new(host, port.to_i)
        @members[member_addr] = member
        Logger.info("Member joined (broadcast): #{member_addr}")
        Logger.debug { "Current member list: #{@members.keys.join(', ')}" }
      rescue => e
        Logger.error("Error handling member joined #{member_addr}: #{e.message}")
        Logger.debug(e.backtrace.join("\n"))
      end
    end

    def handle_ping_req(message)
      return unless message && message['target']
      
      begin
        target_host, target_port = message['target'].split(':')
        return unless target_host && target_port && target_port.to_i.positive?

        ping_message = {
          'type' => 'ping',
          'source' => "#{@host}:#{@port}",
          'timestamp' => Time.now.to_f
        }

        send_message(target_host, target_port.to_i, ping_message)
      rescue => e
        Logger.error("Error handling ping request: #{e.message}")
        Logger.debug(e.backtrace.join("\n"))
      end
    end

    def handle_leave(message)
      return unless message && message['source']
      source = message['source']
      return if source == "#{@host}:#{@port}"

      if member = @members[source]
        old_status = member.status
        member.status = :dead
        notify_member_change(source, old_status, :dead)
        Logger.info("Member left: #{source}")
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
  end
end
