require 'socket'
require 'concurrent'
require 'wisper'
require_relative 'message'
require_relative 'codec'
require_relative 'logger'

module Swim
  class Network
    include Wisper::Publisher

    RECV_BUFFER_SIZE = 65535

    attr_reader :host, :port, :socket

    def initialize(host, port, codec = MessagePackCodec.new)
      @host = host
      @port = port.to_i
      @codec = codec
      @running = false
      @socket = nil
      @receive_thread = nil
      @mutex = Mutex.new
      Logger.info("Network initialized on #{@host}:#{@port} with codec #{@codec.class}")
    end

    def start
      @mutex.synchronize do
        return if running?
        Logger.info("Starting network on #{@host}:#{@port}")
        @socket = UDPSocket.new
        @socket.bind(host, port)
        @running = true
        Logger.info("Network started successfully")
      end
      start_receive_loop
    rescue => e
      Logger.error("Failed to start network: #{e.message}")
      Logger.debug(e.backtrace.join("\n"))
      raise
    end

    def stop
      @mutex.synchronize do
        return unless running?
        Logger.info("Stopping network on #{@host}:#{@port}")
        @running = false
        # Wait for receive thread to finish gracefully
        @receive_thread&.join(1) # Give it 1 second to finish
        @receive_thread&.kill if @receive_thread&.alive?
        @socket&.close rescue nil
        @socket = nil
        Logger.info("Network stopped successfully")
      end
    rescue => e
      Logger.error("Error stopping network: #{e.message}")
      Logger.debug(e.backtrace.join("\n"))
      raise
    end

    def running?
      @running
    end

    def send_message(message, target_host, target_port)
      return false unless running?
      return false unless message.is_a?(Message)

      begin
        Logger.debug("Encoding message: #{message.inspect}")
        data = @codec.encode(message)
        return false unless data

        Logger.debug("Sending #{message.type} message to #{target_host}:#{target_port}")
        bytes_sent = @socket.send(data, 0, target_host, target_port)
        Logger.info("Sent #{message.type} message to #{target_host}:#{target_port} (#{bytes_sent} bytes)")
        broadcast(:message_sent, message, target_host, target_port, bytes_sent)
        true
      rescue => e
        Logger.error("Failed to send message to #{target_host}:#{target_port}: #{e.message}")
        Logger.debug(e.backtrace.join("\n"))
        broadcast(:send_error, e, message, target_host, target_port)
        false
      end
    end

    def broadcast_message(message, targets)
      return 0 unless running?
      return 0 unless message.is_a?(Message)
      return 0 if targets.empty?

      Logger.info("Broadcasting #{message.type} message to #{targets.size} targets")
      Logger.debug("Targets: #{targets.inspect}")

      successful = 0
      targets.each do |addr|
        host, port = addr.split(':')
        if send_message(message, host, port.to_i)
          successful += 1
        end
      end

      Logger.info("Successfully sent to #{successful}/#{targets.size} targets")
      successful
    end

    private

    def start_receive_loop
      Logger.info("Starting receive loop")
      @receive_thread = Thread.new do
        Thread.current.name = "Network Receiver #{@host}:#{@port}"
        Logger.debug("Receive thread started")

        while running?
          begin
            Logger.debug("Waiting for data")
            # Check if socket is still valid
            break unless @socket && !@socket.closed?
            
            # Use timeout to allow graceful shutdown
            ready = IO.select([@socket], nil, nil, 1)
            next unless ready
            
            data, sender = @socket.recvfrom(RECV_BUFFER_SIZE)
            next unless data && sender

            remote_ip = sender[3]
            remote_port = sender[1]
            Logger.debug("Received data from #{remote_ip}:#{remote_port}")

            handle_received_data(data, sender)
          rescue IOError, Errno::EBADF => e
            # Handle closed socket gracefully
            break unless running?
            Logger.debug("Socket closed: #{e.message}")
          rescue => e
            Logger.error("Error in receive loop: #{e.message}")
            Logger.debug(e.backtrace.join("\n"))
            broadcast(:receive_error, e)
            # Add small sleep to prevent tight loop in case of persistent errors
            sleep 0.1
          end
        end
        Logger.info("Receive loop stopped")
      end
    end

    def handle_received_data(data, sender)
      remote_ip = sender[3]
      remote_port = sender[1]
      Logger.debug("Decoding message from #{remote_ip}:#{remote_port}")

      message = @codec.decode(data)
      return unless message

      remote_addr = "#{remote_ip}:#{remote_port}"
      Logger.info("Received #{message.type} message from #{remote_addr}")
      Logger.debug("Message details: #{message.inspect}")

      broadcast(:message_received, message, remote_addr)
    rescue => e
      Logger.error("Failed to decode message from #{remote_ip}:#{remote_port}: #{e.message}")
      Logger.debug("Raw data: #{data.inspect}")
      Logger.debug(e.backtrace.join("\n"))
      broadcast(:decode_error, e, data, sender)
    end
  end
end
