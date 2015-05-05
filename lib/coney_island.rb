module ConeyIsland

  ### BEGIN configuration

  def self.amqp_connection
    @connection
  end

  def self.amqp_parameters=(params)
    @amqp_parameters = params
  end

  def self.amqp_parameters
    @amqp_parameters
  end

  def self.tcp_connection_retries=(number)
    @tcp_connection_retries = number
  end

  def self.tcp_connection_retries
    @tcp_connection_retries
  end

  def self.tcp_connection_retry_limit=(limit)
    @tcp_connection_retry_limit = limit
  end

  def self.tcp_connection_retry_limit
    @tcp_connection_retry_limit ||= 6
  end

  def self.tcp_connection_retry_interval=(interval)
    @tcp_connection_retry_interval = interval
  end

  def self.tcp_connection_retry_interval
    @tcp_connection_retry_interval ||= 10
  end

  def self.notifier
    @notifier ||= "ConeyIsland::Notifiers::#{self.config[:notifier_service]}Notifier".constantize
  end

  def self.config=(config_hash)
    self.amqp_parameters = config_hash.delete :amqp_connection
    if !self.single_amqp_connection?
      ConeyIsland::Submitter.amqp_parameters = config_hash.delete :amqp_connection_submitter
      ConeyIsland::Worker.amqp_parameters = config_hash.delete :amqp_connection_worker
    end
    ConeyIsland::Worker.config=(config_hash)
  end

  def self.config
    ConeyIsland::Worker.config
  end

  def self.delay_seed
    @delay_seed ||= 2
  end

  def self.delay_seed=(seed)
    @delay_seed = seed
  end

  def self.single_amqp_connection?
    !!self.amqp_parameters
  end

  def self.initialize_background
    ConeyIsland::Worker.initialize_background
  end

  def self.start_worker
    ConeyIsland::Worker.start
  end

  def self.run_inline
    ConeyIsland::Submitter.run_inline
  end

  def self.running_inline?
    ConeyIsland::Submitter.running_inline?
  end

  def self.stop_running_inline
    ConeyIsland::Submitter.stop_running_inline
  end

  def self.cache_jobs
    ConeyIsland::Submitter.cache_jobs
  end

  def self.stop_caching_jobs
    ConeyIsland::Submitter.stop_caching_jobs
  end

  def self.flush_jobs
    ConeyIsland::Submitter.flush_jobs
  end

  def self.run_with_em(klass, method, *args)
    ConeyIsland::Submitter.run_with_em(klass, method, *args)
  end

  def self.submit(*args)
    ConeyIsland::Submitter.submit(*args)
  end

  def self.poke_the_badger(message, context, attempts = 1)
    Timeout::timeout(3) do
      self.notifier.notify(message, context)
    end
  rescue
    if attempts <= 3
      attempts += 1
      retry
    end
  end

  def self.default_settings
    { work_queue: 'default', timeout: 30, delay: 0 }
  end

end

require 'coney_island/notifiers/honeybadger_notifier'
require 'coney_island/worker'
require 'coney_island/job'
require 'coney_island/submitter'
require 'coney_island/job_argument_error'
require 'coney_island/railtie' if defined?(Rails)
require 'coney_island/performer'
