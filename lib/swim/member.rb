require 'msgpack'
require 'time'

module Swim
  class Member
    PING_TIMEOUT = 5  # seconds
    SUSPICIOUS_TIMEOUT = 10  # seconds
    FAILED_TIMEOUT = 30  # seconds

    attr_reader :host, :port, :status, :incarnation
    attr_accessor :last_response, :pending_ping

    def initialize(host, port)
      @host = host
      @port = port.to_i
      @status = 'alive'
      @incarnation = 0
      @last_response = nil
      @pending_ping = nil
      @status_changed_at = Time.now
    end

    def address
      "#{@host}:#{@port}"
    end

    def alive?
      @status == 'alive'
    end

    def suspicious?
      @status == 'suspicious'
    end
    alias_method :suspect?, :suspicious?

    def failed?
      @status == 'failed'
    end
    alias_method :dead?, :failed?

    def mark_alive
      return if @status == 'alive'
      @status = 'alive'
      @status_changed_at = Time.now
      @incarnation += 1
      @pending_ping = nil
    end

    def mark_suspicious
      return if @status == 'suspicious'
      @status = 'suspicious'
      @status_changed_at = Time.now
    end

    def mark_failed
      return if @status == 'failed'
      @status = 'failed'
      @status_changed_at = Time.now
      @pending_ping = nil
    end

    def status=(new_status)
      return if @status == new_status
      @status = new_status
      @status_changed_at = Time.now
      @incarnation += 1 if new_status == 'alive'
      @pending_ping = nil if new_status != 'alive'
    end

    def check_timeouts
      now = Time.now

      # 检查 ping 超时
      if @pending_ping && now - @pending_ping > PING_TIMEOUT
        mark_suspicious
        @pending_ping = nil
      end

      # 检查可疑状态超时
      if suspicious? && now - @status_changed_at > SUSPICIOUS_TIMEOUT
        mark_failed
      end

      # 检查失败状态超时
      if failed? && now - @status_changed_at > FAILED_TIMEOUT
        true  # 返回 true 表示应该从成员列表中移除
      else
        false
      end
    end

    def to_h
      {
        host: @host,
        port: @port,
        status: @status,
        incarnation: @incarnation,
        last_response: @last_response&.to_i,
        pending_ping: @pending_ping&.to_i
      }
    end
  end
end
