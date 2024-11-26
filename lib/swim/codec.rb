require 'msgpack'
module Swim
  class Codec
    def encode(message)
      raise NotImplementedError, "#{self.class} must implement encode"
    end

    def decode(data)
      raise NotImplementedError, "#{self.class} must implement decode"
    end

    private

    def symbolize_keys(hash)
      return hash unless hash.is_a?(Hash)
      hash.each_with_object({}) do |(key, value), result|
        result[key.to_sym] = case value
          when Hash then symbolize_keys(value)
          when Array then value.map { |v| v.is_a?(Hash) ? symbolize_keys(v) : v }
          else value
        end
      end
    end
  end

  class MessagePackCodec < Codec
    def encode(message)
      return nil unless message.is_a?(Message)
      message.to_h.to_msgpack
    end

    def decode(data)
      return nil if data.nil? || data.empty?
      
      hash = MessagePack.unpack(data)
      return nil unless hash.is_a?(Hash)
      
      Message.new(
        hash['type'],
        hash['sender'],
        hash['target'],
        symbolize_keys(hash['data'] || {})
      )
 
    end
  end

  class JsonCodec < Codec
    def encode(message)
      return nil unless message.is_a?(Message)
      message.to_h.to_json
    end

    def decode(data)
      return nil if data.nil? || data.empty?
      
      hash = JSON.parse(data)
      return nil unless hash.is_a?(Hash)
      
      Message.new(
        hash['type'],
        hash['sender'],
        hash['target'],
        symbolize_keys(hash['data'] || {})
      )

    end
  end
end
