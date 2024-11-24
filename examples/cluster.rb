#!/usr/bin/env ruby

require 'bundler/setup'
require 'swim'
require 'optparse'

class ClusterExample
  def initialize
    @nodes = {}
    @base_port = 3000
    @node_count = 5
    parse_options
  end

  def parse_options
    OptionParser.new do |opts|
      opts.banner = "Usage: cluster.rb [options]"

      opts.on("-n", "--nodes NUMBER", Integer, "Number of nodes (default: 5)") do |n|
        @node_count = n
      end

      opts.on("-p", "--port PORT", Integer, "Base port number (default: 3000)") do |p|
        @base_port = p
      end

      opts.on("-h", "--help", "Show this help message") do
        puts opts
        exit
      end
    end.parse!
  end

  def start
    puts "Starting a SWIM cluster with #{@node_count} nodes..."
    puts "Base port: #{@base_port}"
    puts "HTTP ports: #{@base_port + 1000} to #{@base_port + 1000 + @node_count - 1}"
    puts

    # Start the first node (seed node)
    first_node = create_node(0, [])
    first_node.start
    puts "Started seed node at localhost:#{@base_port}"
    sleep(1)

    # Start other nodes
    (@node_count - 1).times do |i|
      node_index = i + 1
      seeds = ["localhost:#{@base_port}"] # Use first node as seed
      node = create_node(node_index, seeds)
      node.start
      puts "Started node #{node_index + 1} at localhost:#{@base_port + node_index}"
      sleep(0.5) # Small delay between node starts
    end

    puts "\nAll nodes started! Cluster is forming..."
    puts "\nMonitoring endpoints:"
    @node_count.times do |i|
      puts "Node #{i + 1}: http://localhost:#{@base_port + 1000 + i}"
    end
    puts "\nPress Ctrl+C to stop the cluster"

    # Keep the script running and handle graceful shutdown
    trap('INT') do
      puts "\nShutting down cluster..."
      stop
      exit
    end

    # Wait forever
    sleep
  rescue Interrupt
    # Handle Ctrl+C
    stop
  end

  def stop
    @nodes.each do |port, node|
      puts "Stopping node at localhost:#{port}"
      node.stop
    end
    puts "All nodes stopped"
  end

  private

  def create_node(index, seeds)
    port = @base_port + index
    metadata = {
      'node_index' => index,
      'node_name' => "node_#{index + 1}"
    }
    
    node = Swim.create_node(
      'localhost',
      port,
      seeds,
      metadata,
      enable_http: true
    )
    
    @nodes[port] = node
    node
  end
end

# Start the cluster if this file is being run directly
if __FILE__ == $0
  ClusterExample.new.start
end
