require 'msgpack'
require 'time'

module Swim
  class Member
    attr_reader :host, :port, :incarnation, :status
    attr_accessor :last_response

    def initialize(host, port, incarnation = 0)
      @host = host
      @port = port
      @incarnation = incarnation
      @status = :alive
      @last_response = Time.now
      @last_state_change = Time.now
    end

    def address
      "#{@host}:#{@port}"
    end

    def mark_alive
      update_status(:alive)
    end

    def mark_suspicious
      update_status(:suspicious)
    end

    def mark_failed
      update_status(:failed)
    end

    def alive?
      @status == :alive
    end

    def suspicious?
      @status == :suspicious
    end

    def failed?
      @status == :failed
    end

    def to_msgpack
      {
        'host' => @host,
        'port' => @port,
        'incarnation' => @incarnation,
        'status' => @status.to_s,
        'last_response' => @last_response.to_f
      }.to_msgpack
    end

    private

    def update_status(new_status)
      return if new_status == @status
      @status = new_status
      @last_state_change = Time.now
      Logger.debug("Member #{address} status changed to #{new_status}")
    end
  end
end
