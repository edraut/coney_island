module ConeyIsland
  class Worker

    CONEY_METHODS = [:carousels, :max_network_retries, :network_retry_interval,
      :subscriber_connection, :logger, :poke_the_badger]

    PREFETCH_COUNT = 20
    WORKER_COUNT   = 1

    class << self
      delegate *CONEY_METHODS, to: ConeyIsland

      attr_accessor :network_retries, :ticket, :channel, :exchange

      def ticket
        (@ticket ||= 'default').to_sym
      end

      def children_pids
        @children_pids ||= []
      end

      def carousel
        carousels[ticket] ||= {}
      end

      def prefetch_count
        carousel.fetch(:prefetch_count) { PREFETCH_COUNT }
      end

      def worker_count
        carousel.fetch(:worker_count) { WORKER_COUNT }
      end

      def children_count
        worker_count - 1
      end

      def network_retries
        @network_retries ||= 0
      end

      def running_jobs
        @running_jobs ||= []
      end

      def delayed_jobs
        @delayed_jobs ||= []
      end

      def clear_running_jobs
        running_jobs.clear
      end

      def reset_children_pids
        children_pids.clear
      end

      def initialize_background
        ENV['NEW_RELIC_AGENT_ENABLED'] = 'false'
        ENV['NEWRELIC_ENABLE'] = 'false'
        self.ticket = ARGV[0] if ARGV[0].present?
        reset_children_pids
        logger.info "Initialized in background with carousel: #{carousel}"
      end

      def start
        # Fork children processes if we want them so the rest of this
        # runs here and on every fork (Defaults to no forking)
        fork_children
        # Establishes connection to ActiveRecord, Submitter's bunny connection.
        connect_clients!
        # Starts the main EventMachine loop
        run_em_loop
      end

      protected

      def connect_clients!
        defined?(ActiveRecord::Base) and
          ActiveRecord::Base.establish_connection
        # FIXME: Why do we even connect to the submitter?
        ConeyIsland::Submitter.connect!
      end

      def fork_children
        children_count.times do
          child_pid = Process.fork
          unless child_pid
            logger.info "Started child for ticket #{ticket} with pid #{pid}"
            children_pids.push child_pid
          end
        end
      end

      def run_em_loop
        EventMachine.run do
          Signal.trap('INT')  { shutdown('INT')  }
          Signal.trap('TERM') { shutdown('TERM') }

          AMQP.connect(publisher_connection) do |connection|
            logger.info "Worker Connected to AMQP broker. Running #{AMQP::VERSION}"
            connection.on_error do |conn, connection_close|
              logger.error "Worker Handling a connection-level exception."
              logger.error "AMQP class id : #{connection_close.class_id}"
              logger.error "AMQP method id: #{connection_close.method_id}"
              logger.error "Status code   : #{connection_close.reply_code}"
              logger.error "Error message : #{connection_close.reply_text}"
            end
            #Handle a lost connection to rabbitMQ
            connection.on_tcp_connection_loss do |connection, settings|
              logger.warn "Lost rabbit connection, attempting to reconnect..."
              connection.reconnect true, 1
              initialize_rabbit connection
            end
            initialize_rabbit connection
          end # connect
        end # run
      rescue AMQP::TCPConnectionFailed, AMQP::PossibleAuthenticationFailureError
        self.network_retries += 1
        if network_retries >= max_network_retries
          message = "Failed to connect to RabbitMQ #{network_retries} times, bailing out"
          logger.error message
          poke_the_badger $!, code_source: 'ConeyIsland::Worker.start', reason: message
          abandon_and_shutdown
        else
          interval = network_retry_interval(network_retries)
          message = "Worker Failed to connect to RabbitMQ ##{network_retries} time(s), trying again in #{interval} seconds..."
          logger.error message
          sleep interval
          retry
        end
      end

      def initialize_rabbit(connection)
        logger.info('initializing rabbit connection with channel and queue...')
        self.channel = AMQP::Channel.new(connection)

        channel.on_error do |ch, channel_close|
          logger.error "Worker Handling a channel-level exception."
          logger.error "AMQP class id : #{channel_close.class_id}"
          logger.error "AMQP method id: #{channel_close.method_id}"
          logger.error "Status code   : #{channel_close.reply_code}"
          logger.error "Error message : #{channel_close.reply_text}"
        end

        self.exchange = channel.topic('coney_island')
        #send a heartbeat every 15 seconds to avoid aggressive network configurations that close quiet connections
        heartbeat_exchange = channel.fanout('coney_island_heartbeat')
        EventMachine.add_periodic_timer(15) do
          heartbeat_exchange.publish({:instance_name => @ticket})
          handle_missing_children
        end

        self.channel.prefetch @prefetch_count
        @queue = self.channel.queue(ticket, auto_delete: false, durable: true)
        @queue.bind(self.exchange, routing_key: 'carousels.' + @ticket + '.#')
        @queue.subscribe(:ack => true) do |metadata,payload|
          self.handle_incoming_message(metadata,payload)
        end
        self.network_retries = 0
      end

      def handle_incoming_message metadata, payload
        args = JSON.parse(payload)
        job = Job.new(metadata, args)
        job.handle_job unless job.initialization_errors
      rescue StandardError
        metadata.ack if !ConeyIsland.running_inline?
        poke_the_badger $!, code_source: 'ConeyIsland', job_payload: args
        logger.error "ConeyIsland code error, not application code:\n#{$!.inspect}\nARGS: #{args}"
      end

      def handle_missing_children
        children_pids.each do |child_pid|
          begin
            Process.kill 0, child_pid
          rescue Errno::ESRCH => e
            children_pids.push Process.spawn("bundle exec coney_island #{ticket}")
          end
        end
      end

      def abandon_and_shutdown
        logger.info "Lost RabbitMQ connection, abandoning current jobs and shutting down"
        clear_running_jobs
        shutdown 'TERM'
      end

      def shutdown signal
        @shutting_down = true

        children_pids.each do |child_pid|
          logger.info "Killing child #{child_pid}"
          Process.kill signal, child_pid
        end

        queue.unsubscribe rescue nil
        delayed_jobs.each &:requeue_delay

        EventMachine.add_periodic_timer(1) do
          if running_jobs.any?
            logger.info "Waiting for #{running_jobs.length} requests to finish"
          else
            logger.info "Shutting down coney island #{ticket}"
            EventMachine.stop
          end
        end
      end

    end # class << self

  end
end
