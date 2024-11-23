require 'xxhash'
module Swim
  class HashRing
    DEFAULT_REPLICAS = 160

    def initialize(nodes = [], replicas = DEFAULT_REPLICAS)
      @replicas = replicas
      @ring = {}
      @nodes = []
      nodes.each { |node| add_node(node) }
    end

    def add_node(node)
      @nodes << node
      @replicas.times do |i|
        hash = hash_key("#{node}:#{i}")
        @ring[hash] = node
      end
      @ring = Hash[@ring.sort]
    end

    def remove_node(node)
      @nodes.delete(node)
      @ring.delete_if { |_, v| v == node }
    end

    def get_node(key)
      return nil if @ring.empty?
      hash = hash_key(key)
      node_keys = @ring.keys
      node_key = node_keys.find { |k| k >= hash } || node_keys.first
      @ring[node_key]
    end

    private

    def hash_key(key)
      # Convert the SHA256 hex digest to an integer for consistent hashing
      # Digest::SHA256.hexdigest(key.to_s)[0..7].to_i(16)
      XXhash.xxh32(key.to_s)
    end
  end
end
