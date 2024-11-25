module Swim
  class Message
    VALID_TYPES = [:join, :ping, :ack, :ping_req, :ping_ack, :suspect, :alive, :dead, :metadata].freeze

    attr_reader :type, :sender, :target, :data, :timestamp

    def initialize(type, sender, target = nil, data = {})
      @type = type.to_sym
      validate_type!
      @sender = sender
      @target = target
      @data = data
      @timestamp = Time.now.to_f
    end

    def to_h
      {
        type: @type,
        sender: @sender,
        target: @target,
        data: @data,
        timestamp: @timestamp
      }.compact
    end

    def self.join(sender)
      new(:join, sender)
    end

    def self.ping(sender, target)
      new(:ping, sender, target)
    end

    def self.ack(sender, target)
      new(:ack, sender, target)
    end

    def self.ping_req(sender, target, data)
      new(:ping_req, sender, target, data)
    end

    def self.ping_ack(sender, target, data)
      new(:ping_ack, sender, target, data)
    end

    def self.suspect(sender, target)
      new(:suspect, sender, target)
    end

    def self.alive(sender, target, incarnation = nil)
      new(:alive, sender, target, { incarnation: incarnation }.compact)
    end

    def self.dead(sender, target)
      new(:dead, sender, target)
    end

    def self.metadata(sender, metadata)
      new(:metadata, sender, nil, metadata)
    end

    private

    def validate_type!
      unless VALID_TYPES.include?(@type)
        raise ArgumentError, "Invalid message type: #{@type}. Valid types are: #{VALID_TYPES.join(', ')}"
      end
    end
  end
end
