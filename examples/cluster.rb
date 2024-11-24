#!/usr/bin/env ruby

require 'bundler/setup'
require_relative '../lib/swim'
require 'optparse'

class ClusterExample
  def initialize
    @nodes = {}
    @base_port = 3000
    @node_count = 5
    @log_level = 'INFO'
    parse_options
  end

  def parse_options
    options = {
      base_port: @base_port,
      nodes: @node_count,
      log_level: @log_level
    }

    OptionParser.new do |opts|
      opts.banner = "Usage: cluster.rb [options]"

      opts.on("-p", "--base-port PORT", Integer, "Base port number (default: #{options[:base_port]})") do |p|
        options[:base_port] = p
      end

      opts.on("-n", "--nodes COUNT", Integer, "Number of nodes (default: #{options[:nodes]})") do |n|
        options[:nodes] = n
      end

      opts.on("-l", "--log-level LEVEL", String, "Log level (DEBUG, INFO, WARN, ERROR, FATAL) (default: #{options[:log_level]})") do |level|
        level = level.upcase
        unless %w[DEBUG INFO WARN ERROR FATAL].include?(level)
          puts "Invalid log level: #{level}"
          puts "Valid levels are: DEBUG, INFO, WARN, ERROR, FATAL"
          exit 1
        end
        options[:log_level] = level
      end

      opts.on("-h", "--help", "Show this help message") do
        puts opts
        exit
      end
    end.parse!

    @base_port = options[:base_port]
    @node_count = options[:nodes]
    @log_level = options[:log_level]
  end

  def start
    # 设置日志级别
    Swim::Logger.instance.logger.level = ::Logger.const_get(@log_level)

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
    node_id = "#{index + 1}"

    # 生成随机元数据
    random_metadata = generate_random_metadata(port)

    metadata = {
      'default' => {
        # 基本节点信息
        "node_#{node_id}_index" => index,
        "node_#{node_id}_name" => "node_#{node_id}",
        
        # 节点特定信息
        "localhost:#{port}:role" => index == 0 ? "seed" : "member",
        "localhost:#{port}:start_time" => Time.now.to_i,
        "localhost:#{port}:node_id" => node_id,
        "localhost:#{port}:features" => ["swim", "gossip"].join(","),

        # 随机生成的元数据
        **random_metadata
      }
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

  def generate_random_metadata(port)
    metadata = {}
    
    # 系统信息
    metadata["localhost:#{port}:cpu_cores"] = rand(1..16)
    metadata["localhost:#{port}:memory_gb"] = rand(4..64)
    metadata["localhost:#{port}:disk_gb"] = rand(100..1000)
    
    # 负载信息
    metadata["localhost:#{port}:cpu_usage"] = rand(0.0..100.0).round(2)
    metadata["localhost:#{port}:memory_usage"] = rand(0.0..100.0).round(2)
    metadata["localhost:#{port}:disk_usage"] = rand(0.0..100.0).round(2)
    
    # 网络信息
    metadata["localhost:#{port}:network_latency"] = rand(1..100)
    metadata["localhost:#{port}:bandwidth_mbps"] = [100, 1000, 10000].sample
    metadata["localhost:#{port}:packets_per_second"] = rand(100..10000)
    
    # 服务健康信息
    metadata["localhost:#{port}:health_score"] = rand(0.0..1.0).round(3)
    metadata["localhost:#{port}:error_rate"] = rand(0.0..0.1).round(4)
    metadata["localhost:#{port}:success_rate"] = rand(0.9..1.0).round(4)
    
    # 服务能力
    metadata["localhost:#{port}:max_connections"] = rand(100..10000)
    metadata["localhost:#{port}:current_connections"] = rand(0..100)
    metadata["localhost:#{port}:requests_per_second"] = rand(10..1000)
    
    # 地理信息
    regions = ['us-east', 'us-west', 'eu-west', 'eu-central', 'ap-south', 'ap-northeast']
    zones = ['a', 'b', 'c']
    metadata["localhost:#{port}:region"] = regions.sample
    metadata["localhost:#{port}:zone"] = zones.sample
    metadata["localhost:#{port}:datacenter"] = "dc-#{rand(1..5)}"
    
    # 版本信息
    metadata["localhost:#{port}:version"] = "#{rand(1..3)}.#{rand(0..9)}.#{rand(0..9)}"
    metadata["localhost:#{port}:build"] = format('%08x', rand(16**8))
    metadata["localhost:#{port}:commit"] = format('%040x', rand(16**40))
    
    metadata
  end
end

# Start the cluster if this file is being run directly
if __FILE__ == $0
  ClusterExample.new.start
end
