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

    def initialize
      @logger = ::Logger.new(STDOUT)
      @logger.level = ::Logger::INFO
      
      @logger.formatter = proc do |severity, datetime, progname, msg|
        color = SEVERITY_COLORS[severity] || :default
        time = datetime.strftime("%Y-%m-%d %H:%M:%S.%L")
        prefix = "[#{severity}] #{time}"
        
        # Add process and thread information in debug mode
        if @logger.level == ::Logger::DEBUG
          prefix += " [PID:#{Process.pid} TID:#{Thread.current.object_id}]"
        end
        
        "#{prefix.colorize(color)} #{msg}\n"
      end
    end

    class << self
      def debug(msg)
        instance.logger.debug(msg)
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

      def debug!
        instance.logger.level = ::Logger::DEBUG
      end

      def info!
        instance.logger.level = ::Logger::INFO
      end
    end
  end
end
