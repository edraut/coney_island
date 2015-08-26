require 'coney_island/notifiers/airbrake_notifier'
require 'coney_island/notifiers/honeybadger_notifier'

module ConeyIsland

  class Configuration

    DEFAULT_CONNECTION = { host: '127.0.0.1' }
    DEFAULT_QUEUES = {
      default:   { prefetch_count: 3 },
      cyclone:   { prefetch_count: 3 },
      boardwalk: { prefetch_count: 1 }
    }
    DEFAULT_NETWORK_RETRIES = 6
    DEFAULT_NETWORK_RETRY_SEED = 2
    DEFAULT_DELAY_SEED = 2
    DEFAULT_NOTIFIER = Notifiers::HoneybadgerNotifier

    attr_accessor :connection, :publisher_connection, :subscriber_connection,
      :carousels, :max_network_retries, :network_retry_seed,
      :network_retry_interval, :delay_seed, :notifier, :log_level

    def connection
      @connection ||= DEFAULT_CONNECTION
    end

    def publisher_connection
      @publisher_connection || self.connection
    end

    def subscriber_connection
      @subscriber_connection || self.connection
    end

    def carousels
      @carousels ||= DEFAULT_QUEUES
    end

    def max_network_retries
      @max_network_retries ||= DEFAULT_NETWORK_RETRIES
    end

    def network_retry_seed
      @network_retry_seed ||= DEFAULT_NETWORK_RETRY_SEED
    end

    def network_retry_interval(retries)
      self.network_retry_seed ** retries
    end

    def delay_seed
      @delay_seed ||= DEFAULT_DELAY_SEED
    end

    def notifier
      @notifier ||= DEFAULT_NOTIFIER
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
