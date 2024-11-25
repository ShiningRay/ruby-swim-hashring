require 'msgpack'
require 'time'

module Swim
    # Represents a member of the SWIM cluster.
    #
    # This class encapsulates the state and behavior of a member in a SWIM
    # cluster, including handling of status transitions, timeout checks, and
    # serialization. It manages the member's address, status, incarnation
    # number, and timestamps for last response and state changes.
    #
    # Constants:
    # - PING_TIMEOUT: Duration in seconds before a member is marked as suspicious.
    # - SUSPICIOUS_TIMEOUT: Duration in seconds before a suspicious member is marked as failed.
    # - FAILED_TIMEOUT: Duration in seconds for handling failed state.
    #
    # Attributes:
    # - host: The host address of the member.
    # - port: The port number of the member.
    # - incarnation: The incarnation number, used for causal ordering of state changes.
    # - last_state_change_at: Timestamp of the last state change.
    # - last_response: The timestamp of the last response received.
    # - pending_ping: Timestamp of the last ping sent.
    #
    # The member can also be asked to check for timeouts. If the member has not
    # received a response from another member in a while, it will mark itself as
    # suspect. If the member is already suspect and has not received a response
    # in a longer while, it will mark itself as dead.
    #
    # The member can be serialized to a hash or to msgpack. The serialized form
    # includes the member's host, port, state, incarnation number, last response
    # time, and last state change time.
    #
    # The member can also be cloned. The clone will have the same state as the
    # original, but will not share any instance variables with the original.
  class Member
    PING_TIMEOUT = 5  # seconds
    SUSPICIOUS_TIMEOUT = 10  # seconds
    FAILED_TIMEOUT = 30  # seconds

    attr_accessor :host, :port, :incarnation, :last_state_change_at, :last_response, :pending_ping

    def initialize(host, port, incarnation = 0)
      @host = host
      @port = port.to_i
      @state = :alive
      @incarnation = incarnation
      @last_response = Time.now
      @last_state_change_at = Time.now.to_f
      @pending_ping = nil
    end

    def address
      "#{@host}:#{@port}"
    end

    def state
      @state
    end

    def status
      @state
    end

    def status=(new_status)
      new_status = new_status.to_sym if new_status.is_a?(String)
      raise ArgumentError, "Invalid status: #{new_status}" unless [:alive, :suspect, :dead].include?(new_status)
      @state = new_status
      @last_state_change_at = Time.now.to_f
    end

    def alive?
      @state == :alive
    end

    def suspicious?
      @state == :suspect
    end
    alias_method :suspect?, :suspicious?

    def failed?
      @state == :dead
    end
    alias_method :dead?, :failed?

    def mark_alive
      return if @state == :alive
      @state = :alive
      @last_state_change_at = Time.now.to_f
      @incarnation += 1
    end

    def mark_suspicious
      return if @state == :suspect
      @state = :suspect
      @last_state_change_at = Time.now.to_f
    end

    def mark_failed
      return if @state == :dead
      @state = :dead
      @last_state_change_at = Time.now.to_f
    end

    def update(new_state, new_incarnation)
      return if new_incarnation < @incarnation
      
      if new_incarnation > @incarnation
        @state = new_state
        @incarnation = new_incarnation
        @last_state_change_at = Time.now.to_f
        return
      end
      
      # Same incarnation, only update to more severe state
      state_severity = { alive: 0, suspect: 1, dead: 2 }
      current_severity = state_severity[@state]
      new_severity = state_severity[new_state]
      
      if new_severity && new_severity > current_severity
        @state = new_state
        @last_state_change_at = Time.now.to_f
      end
    end


    def check_timeouts
      now = Time.now.to_f
      
      case @state
      when :alive
        if @last_response && (now - @last_response.to_f) > PING_TIMEOUT
          mark_suspicious
          true
        else
          false
        end
      when :suspect
        if (now - @last_state_change_at) > SUSPICIOUS_TIMEOUT
          mark_failed
          true
        else
          false
        end
      when :dead
        false
      end
    end

    def pending_ping?
      !@pending_ping.nil?
    end

    def clear_pending_ping
      @pending_ping = nil
    end

    def set_pending_ping
      @pending_ping = Time.now
    end

    def clone
      member = super
      member.instance_variable_set(:@last_response, @last_response)
      member.instance_variable_set(:@last_state_change_at, @last_state_change_at)
      member.instance_variable_set(:@pending_ping, @pending_ping)
      member
    end

    def to_h
      {
        host: @host,
        port: @port,
        status: @state,
        incarnation: @incarnation,
        last_response: @last_response&.iso8601,
        last_state_change_at: @last_state_change_at,
        pending_ping: @pending_ping&.iso8601
      }
    end

    def to_msgpack
      to_h.to_msgpack
    end
  end
end
