module Swim
    # Represents a message in the SWIM protocol.
    #
    # The Message class is instantiated with a type, sender and target. The type
    # must be one of the following:
    #
    # * :join
    # * :ping
    # * :ack
    # * :ping_req
    # * :ping_ack
    # * :suspect
    # * :alive
    # * :dead
    # * :metadata
    #
    # The sender and target are host:port strings. The target may be nil in
    # cases where the message is not addressed to a specific node.
    #
    # The data attribute is a hash of arbitrary data that is associated with the
    # message. The timestamp attribute is set to the current time when the
    # message is instantiated.
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

    # Creates an :alive message, which indicates that a node is alive.
    def self.alive(sender, target, incarnation = nil)
      new(:alive, sender, target, { incarnation: incarnation }.compact)
    end

    # Creates a :dead message, which is sent by a node to indicate that another
    # node has failed.
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
