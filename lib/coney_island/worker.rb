module ConeyIsland
  class Worker

    def self.config=(config_hash)
      @config = config_hash.symbolize_keys!
    end

    def self.config
      @config
    end

    def self.log
      @log ||= Logger.new(File.open(File::NULL, "w"))
    end

    def self.log=(log_thing)
      @log = log_thing
    end

    def self.tcp_connection_retries=(number)
      @tcp_connection_retries = number
    end

    def self.tcp_connection_retries
      @tcp_connection_retries
    end

    def self.running_jobs
      @running_jobs ||= []
    end

    def self.clear_running_jobs
      @running_jobs = []
    end

    def self.delayed_jobs
      @delayed_jobs ||= []
    end

    def self.ticket
      @ticket
    end

    def self.ticket=(some_ticket)
      @ticket = some_ticket
    end

    def self.reset_child_pids
      @child_pids = []
    end

    def self.initialize_background
      ENV['NEW_RELIC_AGENT_ENABLED'] = 'false'
      ENV['NEWRELIC_ENABLE'] = 'false'
      @ticket = ARGV[0]
      @ticket ||= 'default'

      @log_io = self.config[:log]
      self.log = Logger.new(@log_io)

      @instance_config = self.config[:carousels][@ticket.to_sym]

      @prefetch_count = @instance_config[:prefetch_count] if @instance_config
      @prefetch_count ||= 20

      @worker_count = @instance_config[:worker_count] if @instance_config
      @worker_count ||= 1
      @child_count = @worker_count - 1
      reset_child_pids

      @full_instance_name = @ticket

      self.log.level = self.config[:log_level]
      self.log.info("config: #{self.config}")
    end

    def self.exchange
      @exchange
    end

    def self.channel
      @channel
    end

    def self.amqp_parameters=(params)
      @amqp_parameters = params
    end

    def self.amqp_parameters
      return @amqp_parameters if @amqp_paramenters.is_a? Hash
      if ConeyIsland.single_amqp_connection?
        @amqp_parameters = ConeyIsland.amqp_parameters
      else
        @amqp_parameters
      end
      if @amqp_parameters.is_a? String
        @amqp_parameters = AMQP::Settings.parse_connection_uri(@amqp_parameters)
      end
      @amqp_parameters
    end

    def self.start
      @child_count.times do
        child_pid = Process.fork
        unless child_pid
          self.log.info("started child for ticket #{@ticket} with pid #{Process.pid}")
          break
        end
        @child_pids.push child_pid
      end
      defined?(ActiveRecord::Base) and
        ActiveRecord::Base.establish_connection

      begin
        EventMachine.run do

          Signal.trap('INT') do
            self.shutdown('INT')
          end
          Signal.trap('TERM') do
            self.shutdown('TERM')
          end

          AMQP.connect(self.amqp_parameters) do |connection|
            self.log.info("Worker Connected to AMQP broker. Running #{AMQP::VERSION}")
            connection.on_error do |conn, connection_close|
              self.log.error "Worker Handling a connection-level exception."
              self.log.error "AMQP class id : #{connection_close.class_id}"
              self.log.error "AMQP method id: #{connection_close.method_id}"
              self.log.error "Status code   : #{connection_close.reply_code}"
              self.log.error "Error message : #{connection_close.reply_text}"
            end
            #Handle a lost connection to rabbitMQ
            connection.on_tcp_connection_loss do |connection, settings|
              self.log.warn("Lost rabbit connection, attempting to reconnect...")
              connection.reconnect(true, 1)
              self.initialize_rabbit(connection)
            end

            self.initialize_rabbit(connection)
          end
        end
      rescue AMQP::TCPConnectionFailed, AMQP::PossibleAuthenticationFailureError => e
        self.tcp_connection_retries ||= 0
        self.tcp_connection_retries += 1
        if self.tcp_connection_retries >= ConeyIsland.tcp_connection_retry_limit
          message = "Failed to connect to RabbitMQ #{ConeyIsland.tcp_connection_retry_limit} times, bailing out"
          self.log.error(message)
          ConeyIsland.poke_the_badger(e, {
            code_source: 'ConeyIsland::Worker.start',
            reason: message}
          )
          self.abandon_and_shutdown
        else
          message = "Worker Failed to connecto to RabbitMQ Attempt ##{self.tcp_connection_retries} time(s), trying again in #{ConeyIsland.tcp_connection_retry_interval(self.tcp_connection_retries)} seconds..."
          self.log.error(message)
          sleep(ConeyIsland.tcp_connection_retry_interval(self.tcp_connection_retries))
          retry
        end
      end
    end

    def self.initialize_rabbit(connection)
      self.log.info('initializing rabbit connection with channel and queue...')
      @channel = AMQP::Channel.new(connection)
      @channel.on_error do |ch, channel_close|
        self.log.error "Worker Handling a channel-level exception."
        self.log.error "AMQP class id : #{channel_close.class_id}"
        self.log.error "AMQP method id: #{channel_close.method_id}"
        self.log.error "Status code   : #{channel_close.reply_code}"
        self.log.error "Error message : #{channel_close.reply_text}"
      end
      @exchange = @channel.topic('coney_island')
      #send a heartbeat every 15 seconds to avoid aggresive network configurations that close quiet connections
      heartbeat_exchange = self.channel.fanout('coney_island_heartbeat')
      EventMachine.add_periodic_timer(15) do
        heartbeat_exchange.publish({:instance_name => @ticket})
        self.handle_missing_children
      end

      self.channel.prefetch @prefetch_count
      @queue = self.channel.queue(@full_instance_name, auto_delete: false, durable: true)
      @queue.bind(self.exchange, routing_key: 'carousels.' + @ticket + '.#')
      if ConeyIsland::Submitter.amqp_connection.respond_to?(:connected?) && !ConeyIsland::Submitter.amqp_connection.connected?
        ConeyIsland::Submitter.handle_connection
      end
      @queue.subscribe(:ack => true) do |metadata,payload|
        self.handle_incoming_message(metadata,payload)
      end
      self.tcp_connection_retries = 0
    end

    def self.handle_incoming_message(metadata,payload)
      args = JSON.parse(payload)
      job = Job.new(metadata, args)
      job.handle_job unless job.initialization_errors
    rescue Exception => e
      metadata.ack if !ConeyIsland.running_inline?
      ConeyIsland.poke_the_badger(e, {code_source: 'ConeyIsland', job_payload: args})
      self.log.error("ConeyIsland code error, not application code:\n#{e.inspect}\nARGS: #{args}")
    end

    def self.handle_missing_children
      @child_pids.each do |child_pid|
        begin
          Process.kill 0, child_pid
        rescue Errno::ESRCH => e
          @child_pids.push Process.spawn("bundle exec coney_island #{@ticket}")
        end
      end
    end

    def self.abandon_and_shutdown
      self.log.info("Lost RabbitMQ connection, abandoning current jobs and shutting down")
      self.clear_running_jobs
      self.shutdown('TERM')
    end

    def self.shutdown(signal)
      @shutting_down = true
      @child_pids.each do |child_pid|
        self.log("killing child #{child_pid}")
        Process.kill(signal, child_pid)
      end
      @queue.unsubscribe rescue nil
      self.delayed_jobs.each do |delayed_job|
        delayed_job.requeue_delay
      end
      EventMachine.add_periodic_timer(1) do
        if self.running_jobs.any?
          self.log.info("Waiting for #{self.running_jobs.length} requests to finish")
        else
          self.log.info("Shutting down coney island #{@ticket}")
          EventMachine.stop
        end
      end
    end

  end
end
