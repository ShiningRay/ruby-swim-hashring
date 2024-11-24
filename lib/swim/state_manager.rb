require 'concurrent'
require 'digest'
require 'msgpack'
require 'securerandom'
require 'logger'

module Swim
  class StateManager
    attr_reader :version, :data

    def initialize(initial_metadata = {})
      @data = Concurrent::Map.new
      @version_vectors = Concurrent::Map.new
      @version = 0
      @mutex = Mutex.new
      @subscribers = []
      @node_id = SecureRandom.uuid
      
      # Initialize with provided metadata
      initial_metadata.each do |namespace, values|
        values.each do |key, value|
          set(key, value, namespace)
        end
      end
      
      start_sync_timer
    end

    def set(key, value, namespace = 'default')
      @mutex.synchronize do
        namespaced_key = "#{namespace}:#{key}"
        old_value = @data[namespaced_key]
        return if old_value == value

        @data[namespaced_key] = value
        update_version_vector(namespaced_key)
        notify_subscribers(namespaced_key, value, :set)
      end
    end

    def get(key, namespace = 'default')
      @data["#{namespace}:#{key}"]
    end

    def delete(key, namespace = 'default')
      @mutex.synchronize do
        namespaced_key = "#{namespace}:#{key}"
        old_value = @data.delete(namespaced_key)
        if old_value
          update_version_vector(namespaced_key)
          notify_subscribers(namespaced_key, nil, :delete)
        end
      end
    end

    def merge_update(updates)
      @mutex.synchronize do
        updates.each do |key, value, operation, remote_vector|
          next if should_skip_update?(key, remote_vector)
          
          case operation.to_sym
          when :set
            @data[key] = value
            @version_vectors[key] = remote_vector
          when :delete
            @data.delete(key)
            @version_vectors[key] = remote_vector
          end
        end
      end
    end

    def subscribe(&block)
      @subscribers << block
    end

    def snapshot
      @mutex.synchronize do
        {
          version: @version,
          data: @data.each_pair.to_h,
          version_vectors: @version_vectors.each_pair.to_h,
          checksum: calculate_checksum
        }
      end
    end

    def apply_snapshot(snapshot)
      @mutex.synchronize do
        if valid_snapshot?(snapshot)
          @data.clear
          snapshot[:data].each { |k, v| @data[k] = v }
          @version_vectors = Concurrent::Map.new(snapshot[:version_vectors])
          @version = snapshot[:version]
          true
        else
          false
        end
      end
    end

    private

    def notify_subscribers(key, value, operation)
      @subscribers.each do |subscriber|
        subscriber.call(key, value, operation)
      end
    end

    def update_version_vector(key)
      vector = @version_vectors[key] || {}
      vector[@node_id] = (vector[@node_id] || 0) + 1
      @version_vectors[key] = vector
      @version += 1
    end

    def should_skip_update?(key, remote_vector)
      return false unless @version_vectors[key]
      
      local_vector = @version_vectors[key]
      # Compare version vectors to detect concurrent updates
      remote_vector.all? { |node, count| (local_vector[node] || 0) >= count }
    end

    def start_sync_timer
      Thread.new do
        loop do
          sleep 5  # Sync every 5 seconds
          begin
            sync_with_peers
          rescue => e
            Logger.error("Sync error: #{e.message}")
          end
        end
      end
    end

    def sync_with_peers
      snapshot = {
        version: @version,
        data: @data.each_pair.to_h,
        version_vectors: @version_vectors.each_pair.to_h,
        checksum: calculate_checksum
      }
      notify_subscribers(:sync, snapshot, :sync)
    end

    def calculate_checksum
      data_string = @data.each_pair.sort.to_s
      Digest::SHA256.hexdigest(data_string)
    end

    def valid_snapshot?(snapshot)
      return false unless snapshot[:version] && snapshot[:data] && snapshot[:checksum] && snapshot[:version_vectors]
      
      @data.clear
      snapshot[:data].each { |k, v| @data[k] = v }
      @version_vectors = Concurrent::Map.new(snapshot[:version_vectors])
      checksum = calculate_checksum
      @data.clear
      @version_vectors.clear
      
      checksum == snapshot[:checksum]
    end
  end
end
