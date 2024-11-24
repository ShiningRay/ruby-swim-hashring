require 'logger'
require 'singleton'
require 'colorize'

module Swim
  class Logger
    include Singleton

    SEVERITY_COLORS = {
      'DEBUG' => :light_black,
      'INFO'  => :light_blue,
      'WARN'  => :yellow,
      'ERROR' => :light_red,
      'FATAL' => :red
    }

    attr_reader :logger
    attr_accessor :node_id

    def initialize
      @logger = ::Logger.new(STDOUT)
      @logger.level = ::Logger::INFO
      @node_id = nil
      
      @logger.formatter = proc do |severity, datetime, progname, msg|
        color = SEVERITY_COLORS[severity] || :default
        time = datetime.strftime("%Y-%m-%d %H:%M:%S.%L")
        prefix = "[#{severity}] #{time}"
        
        # Add node ID if available
        prefix += " [#{@node_id}]" if @node_id
        
        # Add process and thread information in debug mode
        if @logger.level == ::Logger::DEBUG
          prefix += " [PID:#{Process.pid} TID:#{Thread.current.object_id}]"
        end
        
        "#{prefix.colorize(color)} #{msg}\n"
      end
    end

    class << self
      def set_node_id(id)
        instance.node_id = id
      end

      def debug(msg=nil, &block)
        instance.logger.debug(msg, &block)
      end

      def info(msg)
        instance.logger.info(msg)
      end

      def warn(msg)
        instance.logger.warn(msg)
      end

      def error(msg)
        instance.logger.error(msg)
      end

      def fatal(msg)
        instance.logger.fatal(msg)
      end

      def level=(level)
        instance.logger.level = level
      end

      def level
        instance.logger.level
      end
    end
  end
end
