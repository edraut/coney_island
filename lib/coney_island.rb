require 'bunny'
require 'active_support/core_ext/module/delegation'

require 'coney_island/version'
require 'coney_island/configuration'
require 'coney_island/worker'
require 'coney_island/job'
require 'coney_island/submitter'
require 'coney_island/job_argument_error'
require 'coney_island/railtie' if defined?(Rails)
require 'coney_island/performer'

module ConeyIsland

  class << self

    delegate :connection, :publisher_connection, :subscriber_connection,
      :notifier, :max_network_retries, :max_network_retries,
      :network_retry_seed, :network_retry_interval, to: :configuration
    delegate :initialize_background, to: Worker
    delegate :run_inline, :running_inline?, :stop_running_inline, :cache_jobs,
      :stop_caching_jobs, :flush_jobs, :submit, to: Submitter

    def configuration
      @configuration ||= Configuration.new
    end

    def configure
      yield self.configuration if block_given?
    end

    alias :config :configuration

    def start_worker
      ConeyIsland::Worker.start
    end

    def poke_the_badger(message, context, attempts = 1)
      Timeout::timeout(3) do
        self.notifier.notify(message, context)
      end
    rescue
      if attempts <= 3
        attempts += 1
        retry
      end
    end

    def default_settings
      { work_queue: 'default', timeout: 30, delay: 0 }
    end

  end

end
