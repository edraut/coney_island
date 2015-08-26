module ConeyIsland

  # TODO: Implement a client reliable queue, if we lose connection to rabbit
  # we should still store jobs locally and push them once connection is
  # re-established.
  #
  # I want to try to implement the concept of having different adapters
  # for the submitter to use. Initially we'll have the inline and rabbit,
  # later we can expand to have a reliable_rabbit that does rabbit
  # but caches / flushes jobs automatically in case of network issues.
  class Submitter

    CONEY_METHODS = [:publisher_connection, :max_network_retries,
      :network_retry_interval, :poke_the_badger, :logger]

    class << self
      delegate *CONEY_METHODS, to: ConeyIsland

      delegate :store, to: RequestStore

      attr_accessor :connection, :network_retries
      attr_reader :channel, :exchange, :delay_exchange

      def connection
        @connection ||= Bunny.new(publisher_connection)
      end

      def network_retries
        @network_retries ||= 0
      end

      def run_inline
        @run_inline = true
      end

      def stop_running_inline
        @run_inline = false
      end

      def running_inline?
        !!@run_inline
      end

      def cache_jobs
        store[:cache_jobs] = true
      end

      def stop_caching_jobs
        flush_jobs
        store[:cache_jobs] = false
      end

      def caching_jobs?
        !!store[:cache_jobs]
      end

      def flush_jobs
        jobs_cache.each { |id,args| submit! *args, job_id: id }
      end

      def submit *args
        caching_jobs? ? publish_to_cache(args) : submit!(*args)
      end

      def submit! klass, method_name, args: [], instance_id: nil, job_id: nil, timeout: nil, work_queue: nil, delay: 0
        fail ArgumentError, "Expected #{klass} to be a Class or Module" unless [Class, Module].any? {|k| klass.is_a?(k)}
        fail ArgumentError, "Expected #{method_name} to be a String or a Symbol" unless [String,Symbol].any? {|k| method_name.is_a?(k)}

        job_args = {
          args: args,
          instance_id: instance_id,
          timeout: timeout,
          klass: klass.name,
          method_name: method_name
        }.reject { |k,v| v.nil? }

        # Set class defaults if they exist
        if klass.included_modules.include?(Performer)
          delay      ||= klass.get_coney_settings[:delay]
          work_queue ||= klass.get_coney_settings[:work_queue]
        end
        # Set our own defaults if we still don't have any
        work_queue ||= ConeyIsland.default_settings[:work_queue]
        delay      ||= ConeyIsland.default_settings[:delay]

        # Just run this inline if we're not talking to rabbit
        handle_job_inline(job_id,job_args) and return true if running_inline?

        # Make sure we have a connection if we need one
        connect! unless connected?

        # Is this delayed?
        if delay.to_i > 0
          # Publish to the delay exchange
          publish_to_delay_queue job_id, job_args, work_queue, delay
        else
          # Publish to the normal exchange
          publish_to_queue self.exchange, job_id, job_args, work_queue
        end
        true
      rescue StandardError
        logger.error "Error submitting job: #{$!.message}."
        raise $!
      end

      def connected?
        !!connection && connection.connected?
      end

      # We need to rethink how this works and re-raise the exceptions when
      # they are fatal and we can't keep working
      def connect!
        connection.start
        initialize_rabbit
        self.network_retries = 0
      rescue Bunny::TCPConnectionFailed, Bunny::PossibleAuthenticationFailureError
        self.network_retries += 1
        if self.network_retries >= max_network_retries
          message = "Submitter Failed to connect to RabbitMQ #{max_network_retries} times, bailing out"
          on_connection_error message, $!, severity: :warn
          raise $!
        else
          interval = network_retry_interval(self.network_retries)
          message = "Failed to connecto to RabbitMQ Attempt ##{self.network_retries} time(s), trying again in #{interval} seconds..."
          on_connection_error message, $!, severity: :fatal
          retry
        end
      rescue Bunny::ConnectionLevelException, Bunny::ChannelLevelException
        message =  "Submitter Handling a #{$!.class.name} exception: #{$!.message}"
        on_connection_error message, $!
        raise $!
      end

      alias :handle_connection :connect!

      protected

      def on_connection_error(message, exception, severity: :error)
        puts "on_connection_error #{message}, #{exception}, #{severity}"
        logger.send(severity, message)
        poke_the_badger exception, { reason: message }
      end

      def jobs_cache
        RequestStore.store[:jobs]
      end

      def publish_to_cache(args)
        jobs_cache[SecureRandom.uuid] = args
      end

      def initialize_rabbit
        @channel        = self.connection.create_channel
        @exchange       = self.channel.topic('coney_island')
        @delay_exchange = self.channel.topic('coney_island_delay')
        @delay_queue    = {}
      end

      def handle_job_inline(job_id, job_args)
        ConeyIsland::Job.new(job_id, job_args).handle_job
        true
      end

      # Publishes a job to a delayed queue exchange
      def publish_to_delay_queue(job_id, job_args, work_queue, delay)
        @delay_queue[work_queue] ||= {}

        # TODO: Should this be in a different little method, say, bind_delay?
        unless @delay_queue[work_queue][delay].present?
          @delay_queue[work_queue][delay] ||= self.channel.queue(
            work_queue + '_delayed_' + delay.to_s, auto_delete: false, durable: true,
            arguments: {'x-dead-letter-exchange' => 'coney_island', 'x-message-ttl' => delay * 1000})
          @delay_queue[work_queue][delay].bind(self.delay_exchange, routing_key: 'carousels.' + work_queue + ".#{delay}")
        end

        publish_to_queue(self.delay_exchange, job_id, job_args, "#{work_queue}.#{delay}")
      end

      # Publishes a job to a given exchange
      def publish_to_queue(exchange, job_id, job_args, queue)
        exchange.publish(job_args.to_json, {routing_key: "carousels.#{queue}"}) do
          jobs_cache.delete job_id if jobs_cache && job_id.present?
        end
      end

    end

  end
end

