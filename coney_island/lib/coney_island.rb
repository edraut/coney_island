module ConeyIsland

  ### BEGIN configuration

  BG_TIMEOUT_SECONDS = 30

  def self.amqp_connection
    @connection
  end

  def self.amqp_parameters=(params)
    @amqp_parameters = params
  end

  def self.amqp_parameters
    @amqp_parameters
  end

  def self.handle_connection
    @connection ||= AMQP.connect(self.amqp_parameters)
    @channel  ||= AMQP::Channel.new(@connection)
    self.exchange = @channel.topic('coney_island')
  end

  def self.exchange=(amqp_exchange)
    @exchange ||= amqp_exchange
  end

  def self.exchange
    @exchange
  end

  def self.channel
    @channel
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
end

require 'coney_island/notifiers/honeybadger_notifier'
require 'coney_island/worker'
require 'coney_island/submitter'

