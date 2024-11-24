require 'concurrent'
require 'digest'
require 'msgpack'
require 'securerandom'
require 'logger'

module Swim
  class StateManager
    attr_reader :version, :data

    def initialize(initial_metadata = {})
      @state = Concurrent::Map.new
      @subscribers = Concurrent::Array.new
      @version = 0
      @mutex = Mutex.new
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
      return false if key.nil? || namespace.nil?

      @mutex.synchronize do
        ns = @state[namespace] ||= Concurrent::Map.new
        old_value = ns[key]
        ns[key] = value
        @version += 1
        notify_subscribers("#{namespace}:#{key}", value, old_value.nil? ? :add : :update)
        true
      end
    end

    def get(key, namespace = 'default')
      return nil if key.nil? || namespace.nil?
      
      ns = @state[namespace]
      ns&.[](key)
    end

    def delete(key, namespace = 'default')
      return false if key.nil? || namespace.nil?

      @mutex.synchronize do
        ns = @state[namespace]
        if ns && ns.key?(key)
          value = ns.delete(key)
          @version += 1
          notify_subscribers("#{namespace}:#{key}", nil, :delete)
          value
        end
      end
    end

    def subscribe(&block)
      @subscribers << block
    end

    def snapshot
      @mutex.synchronize do
        {
          'state' => @state.each_pair.each_with_object({}) { |(ns, data), hash|
            hash[ns] = data.each_pair.each_with_object({}) { |(k, v), h| h[k] = v }
          },
          'version' => @version
        }
      end
    end

    def merge(snapshot)
      return if !snapshot || !snapshot['state']
      
      @mutex.synchronize do
        # 只有当接收到的版本号更新时才合并
        if snapshot['version'].to_i > @version
          snapshot['state'].each do |namespace, data|
            next if namespace.nil? || !data.is_a?(Hash)
            
            ns = @state[namespace] ||= Concurrent::Map.new
            data.each do |key, value|
              next if key.nil?
              
              old_value = ns[key]
              if old_value != value
                ns[key] = value
                notify_subscribers("#{namespace}:#{key}", value, old_value.nil? ? :add : :update)
              end
            end
          end
          @version = snapshot['version'].to_i
        end
      end
    end

    def merge_update(updates)
      return unless updates.is_a?(Array)

      @mutex.synchronize do
        updates.each do |key, value, operation|
          next if key.nil?
          
          case operation.to_sym
          when :add, :update
            namespace, k = key.split(':', 2)
            next if k.nil?
            
            ns = @state[namespace] ||= Concurrent::Map.new
            old_value = ns[k]
            ns[k] = value
            notify_subscribers(key, value, operation.to_sym)
          when :delete
            namespace, k = key.split(':', 2)
            next if k.nil?
            
            ns = @state[namespace]
            if ns && ns.key?(k)
              ns.delete(k)
              notify_subscribers(key, nil, :delete)
            end
          end
        end
        @version += 1
      end
    end

    def get_namespace(namespace)
      return {} if namespace.nil?
      
      ns = @state[namespace]
      return {} unless ns
      
      ns.each_pair.each_with_object({}) { |(k, v), hash| hash[k] = v }
    end

    private

    def notify_subscribers(key, value, operation)
      @subscribers.each do |subscriber|
        begin
          subscriber.call(key, value, operation)
        rescue => e
          Logger.error("Error in state subscriber: #{e.message}")
        end
      end
    end

    def calculate_checksum
      data_string = @state.each_pair.each_with_object({}) { |(ns, data), hash|
        hash[ns] = data.each_pair.each_with_object({}) { |(k, v), h| h[k] = v }
      }.to_s
      Digest::SHA256.hexdigest(data_string)
    end

    def sync_with_peers
      snapshot = {
        'state' => @state.each_pair.each_with_object({}) { |(ns, data), hash|
          hash[ns] = data.each_pair.each_with_object({}) { |(k, v), h| h[k] = v }
        },
        'version' => @version
      }
      notify_subscribers(:sync, snapshot, :sync)
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

    def valid_snapshot?(snapshot)
      return false unless snapshot['version'] && snapshot['state'] && snapshot['checksum']
      
      @state.clear
      snapshot['state'].each do |namespace, data|
        ns = @state[namespace] ||= Concurrent::Map.new
        data.each do |key, value|
          ns[key] = value
        end
      end
      checksum = calculate_checksum
      @state.clear
      
      checksum == snapshot['checksum']
    end
  end
end
