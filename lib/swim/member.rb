require 'msgpack'
require 'time'

module Swim
  class Member
    attr_reader :host, :port, :incarnation, :state, :last_state_change_at

    def initialize(host, port, incarnation = 0)
      @host = host
      @port = port
      @incarnation = incarnation
      @state = :alive
      @last_state_change_at = Time.now.to_f
    end

    def address
      "#{@host}:#{@port}"
    end

    def update(new_state, new_incarnation)
      return if new_incarnation < @incarnation
      
      if new_incarnation > @incarnation
        @incarnation = new_incarnation
        update_state(new_state)
      else # equal incarnation
        update_state(new_state) if more_severe?(new_state)
      end
    end

    def alive?
      @state == :alive
    end

    def suspect?
      @state == :suspect
    end

    def dead?
      @state == :dead
    end

    def to_msgpack
      {
        'host' => @host,
        'port' => @port,
        'incarnation' => @incarnation,
        'state' => @state.to_s
      }.to_msgpack
    end

    private

    def update_state(new_state)
      @state = new_state
      @last_state_change_at = Time.now.to_f
    end

    def more_severe?(new_state)
      severity = { alive: 0, suspect: 1, dead: 2 }
      severity.fetch(new_state, 0) > severity.fetch(@state, 0)
    end
  end
end
