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
            self.log.info("Connected to AMQP broker. Running #{AMQP::VERSION}")
            @channel = AMQP::Channel.new(connection)
            @exchange = @channel.topic('coney_island')
            #Handle a lost connection to rabbitMQ
            connection.on_tcp_connection_loss do |connection, settings|
              # since we lost the connection, rabbitMQ will resend all jobs we didn't finish
              # so drop them and restart
              unless @shutting_down
                self.abandon_and_shutdown
              end
            end

            #send a heartbeat every 15 seconds to avoid aggresive network configurations that close quiet connections
            heartbeat_exchange = self.channel.fanout('coney_island_heartbeat')
            EventMachine.add_periodic_timer(15) do
              heartbeat_exchange.publish({:instance_name => @ticket})
              self.handle_missing_children
            end

            self.channel.prefetch @prefetch_count
            @queue = self.channel.queue(@full_instance_name, auto_delete: false, durable: true)
            @queue.bind(self.exchange, routing_key: 'carousels.' + @ticket + '.#')
            @queue.subscribe(:ack => true) do |metadata,payload|
              self.handle_incoming_message(metadata,payload)
            end
          end
        end
      rescue AMQP::TCPConnectionFailed => e
        ConeyIsland.tcp_connection_retries ||= 0
          ConeyIsland.tcp_connection_retries += 1
        if ConeyIsland.tcp_connection_retries >= ConeyIsland.tcp_connection_retry_limit
          message = "Failed to connect to RabbitMQ #{ConeyIsland.tcp_connection_retry_limit} times, bailing out"
          self.log.error(message)
          ConeyIsland.poke_the_badger(e, {
            code_source: 'ConeyIsland::Worker.start',
            reason: message}
          )
        else
          message = "Failed to connecto to RabbitMQ Attempt ##{ConeyIsland.tcp_connection_retries} time(s), trying again in #{ConeyIsland.tcp_connection_retry_interval} seconds..."
          self.log.error(message)
          sleep(10)
          retry
        end
      end
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
