module ConeyIsland

  class Submitter

    class << self

      delegate :publisher_connection, :max_network_retries,
        :network_retry_interval, to: ConeyIsland

      attr_writer :connection, :network_retries
      attr_reader :channel, :exchange, :delay_exchange

      def run_inline
        @run_inline = true
      end

      def stop_running_inline
        @run_inline = false
      end

      def running_inline?
        !!@run_inline
      end

      def submit(*args)
        if RequestStore.store[:cache_jobs]
          job_id = SecureRandom.uuid
          RequestStore.store[:jobs][job_id] = args
        else
          self.submit!(args)
        end
      end

      def submit!(args)
        if @run_inline
          self.submit_all!(args)
        else
          begin
            self.submit_all!(args)
          rescue StandardError => e
            Rails.logger.error(e)
            ConeyIsland.poke_the_badger(e,{
              code_source: "ConeyIsland::Submitter.submit!",
              message: "Error submitting job",
              job_args: args
              })
          end
        end
      end

      def submit_all!(args)
        if :all_cached_jobs == args
          Rails.logger.info("ConeyIsland::Submitter.submit! about to iterate over this many jobs: #{RequestStore.store[:jobs].length}")
          RequestStore.store[:jobs].each do |job_id,job_args|
            self.publish_job(job_args,job_id)
          end
        else
          self.publish_job(args)
        end
      end

      def connected?
        !!connection && connection.connected?
      end

      def handle_connection
        Rails.logger.info("ConeyIsland::Submitter.handle_connection connecting...")
        self.connection = Bunny.new(publisher_connection)
        start_connection

      rescue Bunny::TCPConnectionFailed, Bunny::PossibleAuthenticationFailureError => e
        self.network_retries ||= 0
        self.network_retries += 1
        if self.network_retries >= max_network_retries
          message = "Submitter Failed to connect to RabbitMQ #{max_network_retries} times, bailing out"
          Rails.logger.error(message)
          ConeyIsland.poke_the_badger(e, {
            code_source: 'ConeyIsland::Submitter.handle_connection',
            reason: message}
          )
          @connection = nil
        else
          message = "Failed to connecto to RabbitMQ Attempt ##{self.network_retries} time(s), trying again in #{network_retry_interval(self.network_retries)} seconds..."
          Rails.logger.error(message)
          sleep(network_retry_interval(self.network_retries))
          retry
        end
      rescue Bunny::ConnectionLevelException => e
        Rails.logger.error "Submitter Handling a connection-level exception: #{e.message}"
      rescue Bunny::ChannelLevelException => e
        Rails.logger.error "Submitter Handling a channel-level exception: #{e.message}"
      else
        self.initialize_rabbit
        self.network_retries = 0
      end

      def publish_job(args, job_id = nil)
        # Map arguments
        klass, method_name, job_args = *args
        # Job args is optional
        job_args ||= {}

        # Check arguments
        # Break if klass isn't a Class or a Module
        fail ArgumentError, "Expected #{klass} to be a Class or Module" unless [Class, Module].any? {|k| klass.is_a?(k)}
        # Break if method_name isn't a String or a Symbol
        fail ArgumentError, "Expected #{method_name} to be a String or a Symbol" unless [String,Symbol].any? {|k| method_name.is_a?(k)}

        # Set defaults
        job_args['klass']       = klass.name
        job_args['method_name'] = method_name
        job_args.stringify_keys!

        # Extract non job args
        delay      = job_args.delete 'delay'
        work_queue = job_args.delete 'work_queue'

        # Set class defaults if they exist
        if klass.included_modules.include?(Performer)
          delay      ||= klass.get_coney_settings[:delay]
          work_queue ||= klass.get_coney_settings[:work_queue]
        end

        # Set our own defaults if we still don't have any
        work_queue ||= ConeyIsland.default_settings[:work_queue]
        delay      ||= ConeyIsland.default_settings[:delay]

        # Make sure we have a connection if we need one
        handle_connection if !running_inline? && !connected?

        if self.running_inline?
          # Just run this inline if we're not threaded
          ConeyIsland::Job.new(nil, job_args).handle_job
        elsif delay && delay.to_i > 0
          # Is this delayed?
          # Publish to the delay exchange
          publish_to_delay_queue(job_id, job_args, work_queue, delay)
        else
          # Publish to the normal exchange
          publish_to_queue(self.exchange, job_id, job_args, work_queue)
        end

        true
      end

      def cache_jobs
        RequestStore.store[:cache_jobs] = true
        RequestStore.store[:jobs] = {}
      end

      def flush_jobs
        self.submit!(:all_cached_jobs) if RequestStore.store[:jobs].any?
      end

      def stop_caching_jobs
        RequestStore.store[:cache_jobs] = false
      end

      protected

      def initialize_rabbit
        self.create_channel
        @exchange = self.channel.topic('coney_island')
        @delay_exchange = self.channel.topic('coney_island_delay')
        @delay_queue = {}
      end

      def start_connection
        @connection.start
      end

      def create_channel
        @channel = self.connection.create_channel
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
          RequestStore.store[:jobs].delete job_id if RequestStore.store[:jobs] && job_id.present?
        end
      end

    end

  end
end

