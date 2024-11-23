require 'concurrent'
require 'digest'
require 'msgpack'

module Swim
  class StateManager
    attr_reader :version, :data

    def initialize
      @data = Concurrent::Map.new
      @version = 0
      @mutex = Mutex.new
      @subscribers = []
    end

    def set(key, value)
      @mutex.synchronize do
        old_value = @data[key]
        return if old_value == value

        @data[key] = value
        @version += 1
        notify_subscribers(key, value, :set)
      end
    end

    def get(key)
      @data[key]
    end

    def delete(key)
      @mutex.synchronize do
        old_value = @data.delete(key)
        if old_value
          @version += 1
          notify_subscribers(key, nil, :delete)
        end
      end
    end

    def merge_update(updates)
      @mutex.synchronize do
        updates.each do |key, value, operation|
          case operation.to_sym
          when :set
            @data[key] = value
          when :delete
            @data.delete(key)
          end
        end
        @version += 1
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
          checksum: calculate_checksum
        }
      end
    end

    def apply_snapshot(snapshot)
      @mutex.synchronize do
        if valid_snapshot?(snapshot)
          @data.clear
          snapshot[:data].each { |k, v| @data[k] = v }
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

    def calculate_checksum
      data_string = @data.each_pair.sort.to_s
      Digest::SHA256.hexdigest(data_string)
    end

    def valid_snapshot?(snapshot)
      return false unless snapshot[:version] && snapshot[:data] && snapshot[:checksum]
      
      @data.clear
      snapshot[:data].each { |k, v| @data[k] = v }
      checksum = calculate_checksum
      @data.clear
      
      checksum == snapshot[:checksum]
    end
  end
end
