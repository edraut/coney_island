module ConeyIsland
  class Submitter

    def self.run_inline
      @run_inline = true
    end

    def self.stop_running_inline
      @run_inline = false
    end

    def self.running_inline?
      @run_inline
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
        self.handle_connection
        begin
          self.submit_all!(args)
        rescue Exception => e
          Rails.logger.error(e)
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

    def self.channel
      @channel
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

    def self.handle_connection
      @connection ||= AMQP.connect(self.amqp_parameters)
    rescue AMQP::TCPConnectionFailed => e
      ConeyIsland.tcp_connection_retries ||= 0
        ConeyIsland.tcp_connection_retries += 1
      if ConeyIsland.tcp_connection_retries >= ConeyIsland.tcp_connection_retry_limit
        message = "Failed to connect to RabbitMQ #{ConeyIsland.tcp_connection_retry_limit} times, bailing out"
        Rails.logger.error(message)
        ConeyIsland.poke_the_badger(e, {
          code_source: 'ConeyIsland::Submitter.handle_connection',
          reason: message}
        )
      else
        message = "Failed to connecto to RabbitMQ Attempt ##{ConeyIsland.tcp_connection_retries} time(s), trying again in #{ConeyIsland.tcp_connection_retry_interval} seconds..."
        Rails.logger.error(message)
        sleep(10)
        retry
      end
    else
      @channel ||= AMQP::Channel.new(@connection)
      @exchange = @channel.topic('coney_island')
      @delay_exchange = @channel.topic('coney_island_delay')
      @delay_queue = {}
    end

    def self.amqp_connection
      @connection
    end

    def self.publish_job(args, job_id = nil)
      if (args.first.is_a? Class or args.first.is_a? Module) and (args[1].is_a? String or args[1].is_a? Symbol) and args.last.is_a? Hash and 3 == args.length
        klass = args.shift
        klass_name = klass.name

        method_name = args.shift
        job_args = args.shift
        job_args ||= {}
        job_args['klass'] = klass_name
        job_args['method_name'] = method_name
        job_args.stringify_keys!
        if @run_inline
          job = ConeyIsland::Job.new(nil, job_args)
          job.handle_job
        else
          work_queue = job_args.delete 'work_queue'
          if klass.respond_to? :coney_island_settings
            work_queue ||= klass.coney_island_settings[:work_queue]
          end
          work_queue ||= 'default'
          delay = job_args['delay']
          if klass.respond_to? :coney_island_settings
            delay ||= klass.coney_island_settings[:delay]
          end
          if delay && delay.to_i > 0
            @delay_queue[work_queue] ||= {}
            unless @delay_queue[work_queue][delay].present?
              @delay_queue[work_queue][delay] ||= self.channel.queue(
                work_queue + '_delayed_' + delay.to_s, auto_delete: false, durable: true,
                arguments: {'x-dead-letter-exchange' => 'coney_island', 'x-message-ttl' => delay * 1000})
              @delay_queue[work_queue][delay].bind(self.delay_exchange, routing_key: 'carousels.' + work_queue + ".#{delay}")
            end
            self.delay_exchange.publish(job_args.to_json, {routing_key: "carousels.#{work_queue}.#{delay}"}) do
              RequestStore.store[:jobs].delete job_id if RequestStore.store[:jobs] && job_id.present?
            end
          else
            self.exchange.publish(job_args.to_json, {routing_key: "carousels.#{work_queue}"}) do
              RequestStore.store[:jobs].delete job_id if RequestStore.store[:jobs] && job_id.present?
            end
          end
        end
        true
      else
        raise ConeyIsland::JobArgumentError.new
      end
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

    def self.run_with_em(klass, method, *args)
      ConeyIsland.stop_running_inline
      EventMachine.run do
        self.cache_jobs
        klass.send(method, *args)
        self.flush_jobs
        self.publisher_shutdown
      end
      ConeyIsland.run_inline
    end

    def self.publisher_shutdown
      EventMachine.add_periodic_timer(1) do
        if RequestStore.store[:jobs] && RequestStore.store[:jobs].length > 0
          Rails.logger.info("Waiting for #{RequestStore.store[:jobs].length} publishes to finish")
        else
          Rails.logger.info("Shutting down coney island publisher")
          EventMachine.stop
        end
      end
    end
  end
end

