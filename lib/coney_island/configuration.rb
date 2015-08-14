require 'coney_island/notifiers/airbrake_notifier'
require 'coney_island/notifiers/honeybadger_notifier'

module ConeyIsland

  class Configuration

    attr_writer :connection, :publisher_connection, :subscriber_connection,
      :carousels,:amqp_parameters, :tcp_connection_retry_limit,
      :tcp_connection_retry_seed, :tcp_connection_retry_interval,
      :delay_seed, :notifier, :log_level

    def connection
      @connection ||= { host: '127.0.0.1' }
    end

    def publisher_connection
      @publisher_connection || @connection
    end

    def subscriber_connection
      @subscriber_connection || @connection
    end

    def carousels
      @carousels ||= {
        default:   { prefetch_count: 3 },
        cyclone:   { prefetch_count: 3 },
        boardwalk: { prefetch_count: 1 }
      }
    end

    def tcp_connection_retry_limit
      @tcp_connection_retry_limit ||= 6
    end

    def tcp_connection_retry_seed
      @tcp_connection_retry_seed ||= 2
    end

    def delay_seed
      @delay_seed ||= 2
    end

    def tcp_connection_retry_interval(retries)
      self.tcp_connection_retry_seed ** retries
    end

    def notifier
      @notifier ||= Notifiers::HoneybadgerNotifier
    end

    def notifier=(notifier)
      @notifier = case notifier.to_s
      when 'honeybadger'
        Notifiers::HoneybadgerNotifier
      when 'airbrake'
        Notifiers::AirbrakeNotifier
      else
        fail ArgumentError, "value must be :honeybadger or :airbrake. Passed: #{value.inspect}"
      end
    end

  end

end
