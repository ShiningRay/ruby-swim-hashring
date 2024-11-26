#!/usr/bin/env ruby

require 'bundler/setup'
require 'swim'
require 'optparse'
require 'irb'

module Swim
  class Console
    def initialize(options = {})
      @host = options[:host] || '127.0.0.1'
      @port = options[:port] || 7946
      @network = Network.new(@host, @port)
      @network.subscribe(self)
    end

    def start
      @network.start
      puts "Network started on #{@host}:#{@port}"
      puts "Available commands:"
      puts "  network - access the network instance"
      puts "  send_message(target_host, target_port, message) - send a message"
      puts "  broadcast_message(message, targets) - broadcast a message"
      puts "  quit - stop the network and exit"
      
      # 将实例变量暴露给 IRB
      TOPLEVEL_BINDING.eval('@network = @network')
      
      IRB.start
    end

    def stop
      @network.stop
      puts "Network stopped"
    end

    # Event handlers
    def on_message_received(message, remote_addr)
      puts "\nReceived message from #{remote_addr}:"
      puts "  Type: #{message.type}"
      puts "  Sender: #{message.sender}"
      puts "  Target: #{message.target}"
      puts "  Data: #{message.data.inspect}"
    end

    def on_message_sent(message, target_host, target_port, bytes_sent)
      puts "\nSent message to #{target_host}:#{target_port} (#{bytes_sent} bytes):"
      puts "  Type: #{message.type}"
      puts "  Sender: #{message.sender}"
      puts "  Target: #{message.target}"
      puts "  Data: #{message.data.inspect}"
    end

    def on_send_error(error, message, target_host, target_port)
      puts "\nError sending message to #{target_host}:#{target_port}:"
      puts "  Error: #{error.message}"
    end

    def on_receive_error(error)
      puts "\nError receiving message:"
      puts "  Error: #{error.message}"
    end

    def on_decode_error(error, data, sender)
      puts "\nError decoding message from #{sender[3]}:#{sender[1]}:"
      puts "  Error: #{error.message}"
    end
  end
end

# Parse command line options
options = {}
OptionParser.new do |opts|
  opts.banner = "Usage: #{$0} [options]"

  opts.on("-H", "--host HOST", "Host to bind to (default: 127.0.0.1)") do |h|
    options[:host] = h
  end

  opts.on("-p", "--port PORT", Integer, "Port to bind to (default: 7946)") do |p|
    options[:port] = p
  end
end.parse!

# Start console
console = Swim::Console.new(options)

# Handle Ctrl-C gracefully
Signal.trap("INT") do
  puts "\nShutting down..."
  console.stop
  exit
end

console.start
