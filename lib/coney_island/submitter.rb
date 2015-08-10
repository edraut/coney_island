module ConeyIsland
  class Submitter

    def self.run_inline
      @run_inline = true
    end

    def self.stop_running_inline
      @run_inline = false
    end

    def self.running_inline?
      !!@run_inline
    end

    def self.tcp_connection_retries=(number)
      @tcp_connection_retries = number
    end

    def self.tcp_connection_retries
      @tcp_connection_retries
    end

    def self.submit(*args)
      if RequestStore.store[:cache_jobs]
        job_id = SecureRandom.uuid
        RequestStore.store[:jobs][job_id] = args
      else
        self.submit!(args)
      end
    end

    def self.submit!(args)
      if @run_inline
        self.submit_all!(args)
      else
        begin
          self.submit_all!(args)
        rescue StandardError => e
          Rails.logger.error(e)
          Rails.logger.error(e.backtrace.join("\n"))
          Rails.logger.info("Bunny connection: #{self.connection.inspect}, status: #{self.connection.status}")
          ConeyIsland.poke_the_badger(e,{
            code_source: "ConeyIsland::Submitter.submit!",
            message: "Error submitting job",
            job_args: args
            })
        end
      end
    end

    def self.submit_all!(args)
      if :all_cached_jobs == args
        Rails.logger.info("ConeyIsland::Submitter.submit! about to iterate over this many jobs: #{RequestStore.store[:jobs].length}")
        RequestStore.store[:jobs].each do |job_id,job_args|
          self.publish_job(job_args,job_id)
        end
      else
        self.publish_job(args)
      end
    end

    def self.connection=(conn)
      @connection = conn
    end

    def self.connection
      @connection
    end

    def self.start_connection
      @connection.start
    end

    def self.channel
      @channel
    end

    def self.create_channel
      @channel = self.connection.create_channel
    end

    def self.exchange
      @exchange
    end

    def self.delay_exchange
      @delay_exchange
    end

    def self.amqp_parameters=(params)
      @amqp_parameters = params
    end

    def self.amqp_parameters
      if ConeyIsland.single_amqp_connection?
        ConeyIsland.amqp_parameters
      else
        @amqp_parameters
      end
    end

    def self.log
      ConeyIsland::Worker.log
    end

    def self.handle_connection
      if self.connection.present? && self.connection.open?
        log.info "Connection was already open, closing..."
        self.connection.close
      end
      log.info("ConeyIsland::Submitter.handle_connection connecting...")
      self.connection = Bunny.new(self.amqp_parameters)
      log.info("#{Process.pid} Created connection: #{self.connection.inspect}, status: #{self.connection.status}")
      self.start_connection
      log.info("#{Process.pid} Started connection: #{self.connection.inspect}, status: #{self.connection.status}")

    rescue Bunny::TCPConnectionFailed, Bunny::PossibleAuthenticationFailureError => e
      self.tcp_connection_retries ||= 0
      self.tcp_connection_retries += 1
      if self.tcp_connection_retries >= ConeyIsland.tcp_connection_retry_limit
        message = "Submitter Failed to connect to RabbitMQ #{ConeyIsland.tcp_connection_retry_limit} times, bailing out"
        Rails.logger.error(message)
        ConeyIsland.poke_the_badger(e, {
          code_source: 'ConeyIsland::Submitter.handle_connection',
          reason: message}
        )
        @connection = nil
      else
        message = "Failed to connecto to RabbitMQ Attempt ##{self.tcp_connection_retries} time(s), trying again in #{ConeyIsland.tcp_connection_retry_interval(self.tcp_connection_retries)} seconds..."
        Rails.logger.error(message)
        sleep(ConeyIsland.tcp_connection_retry_interval(self.tcp_connection_retries))
        retry
      end
    rescue Bunny::ConnectionLevelException => e
      Rails.logger.error "Submitter Handling a connection-level exception."
      # Rails.logger.error "Bunny class id : #{e.connection_close.class_id}"
      # Rails.logger.error "Bunny method id: #{e.connection_close.method_id}"
      # Rails.logger.error "Status code   : #{e.connection_close.reply_code}"
      # Rails.logger.error "Error message : #{e.connection_close.reply_text}"
    rescue Bunny::ChannelLevelException => e
      Rails.logger.error "Submitter Handling a channel-level exception."
      Rails.logger.error "Bunny class id : #{e.channel_close.class_id}"
      Rails.logger.error "Bunny method id: #{e.channel_close.method_id}"
      Rails.logger.error "Status code   : #{e.channel_close.reply_code}"
      Rails.logger.error "Error message : #{e.channel_close.reply_text}"
    else
      self.initialize_rabbit
      self.tcp_connection_retries = 0
    end

    def self.initialize_rabbit
      self.create_channel
      @exchange = self.channel.topic('coney_island')
      @delay_exchange = self.channel.topic('coney_island_delay')
      @delay_queue = {}
    end

    def self.amqp_connection
      @connection
    end

    def self.publish_job(args, job_id = nil)
      # Map arguments
      klass, method_name, job_args = *args
      # Job args is optional
      job_args ||= {}

      # Check arguments
      # Break if klass isn't a Class or a Module
      raise ConeyIsland::JobArgumentError.new "Expected #{klass} to be a Class or Module" unless [Class, Module].any? {|k| klass.is_a?(k)}
      # Break if method_name isn't a String or a Symbol
      raise ConeyIsland::JobArgumentError.new "Expected #{method_name} to be a String or a Symbol" unless [String,Symbol].any? {|k| method_name.is_a?(k)}

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

    def self.cache_jobs
      RequestStore.store[:cache_jobs] = true
      RequestStore.store[:jobs] = {}
    end

    def self.flush_jobs
      self.submit!(:all_cached_jobs) if RequestStore.store[:jobs].any?
    end

    def self.stop_caching_jobs
      RequestStore.store[:cache_jobs] = false
    end

    protected

    # Publishes a job to a delayed queue exchange
    def self.publish_to_delay_queue(job_id, job_args, work_queue, delay)
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
    def self.publish_to_queue(exchange, job_id, job_args, queue)
      exchange.publish(job_args.to_json, {routing_key: "carousels.#{queue}"}) do
        RequestStore.store[:jobs].delete job_id if RequestStore.store[:jobs] && job_id.present?
      end
    end

  end
end

