module ConeyIsland
  class Submitter

    def self.run_inline
      @run_inline = true
    end

    def self.stop_running_inline
      @run_inline = false
    end

    def self.submit(*args)
      if RequestStore.store[:cache_jobs]
        RequestStore.store[:jobs].push args
      else
        self.submit!(args)
      end
    end

    def self.submit!(args)
      self.handle_connection unless @run_inline
      if :all_cached_jobs == args
        RequestStore.store[:jobs].delete_if do |job_args|
          self.publish_job(job_args)
        end
      else
        self.publish_job(args)
      end
    end

    def self.exchange
      @exchange
    end

    def self.amqp_parameters=(params)
      @amqp_parameters = params
    end

    def self.amqp_parameters
      @amqp_parameters
    end

    def self.handle_connection
      if ConeyIsland.single_amqp_connection?
        Rails.logger.info("using single connection to RabbitMQ")
        ConeyIsland.handle_connection(Rails.logger)
        @exchange = ConeyIsland.exchange
      else
        self.submitter_connection
      end
    end

    def self.submitter_connection
      @connection ||= AMQP.connect(self.amqp_parameters)
    rescue AMQP::TCPConnectionFailed => e
      @tcp_connection_retries ||= 0
        @tcp_connection_retries += 1
      if @tcp_connection_retries >= 6
        message = "Failed to connect to RabbitMQ 6 times, bailing out"
        Rails.logger.error(message)
        ConeyIsland.poke_the_badger(e, {
          code_source: 'ConeyIsland::Submitter.submitter_connection',
          reason: message}
        )
      else
        message = "Failed to connecto to RabbitMQ Attempt ##{@tcp_connection_retries} time(s), trying again in 10 seconds..."
        Rails.logger.error(message)
        ConeyIsland.poke_the_badger(e, {
          code_source: 'ConeyIsland::Submitter.submitter_connection',
          reason: message})
        sleep(10)
        retry
      end
    else
      @channel  ||= AMQP::Channel.new(@connection)
      @exchange = @channel.topic('coney_island')
    end

    def self.publish_job(args)
      if (args.first.is_a? Class or args.first.is_a? Module) and (args[1].is_a? String or args[1].is_a? Symbol) and args.last.is_a? Hash and 3 == args.length
        klass = args.shift
        klass = klass.name
        method_name = args.shift
        job_args = args.shift
        job_args ||= {}
        job_args['klass'] = klass
        job_args['method_name'] = method_name
        if @run_inline
          job_args.stringify_keys!
          ConeyIsland::Worker.execute_job_method(job_args)
        else
          work_queue = job_args.delete :work_queue
          work_queue ||= 'default'
          self.exchange.publish((job_args.to_json), routing_key: "carousels.#{work_queue}")
        end
      end
      true
    end

    def self.cache_jobs
      RequestStore.store[:cache_jobs] = true
      RequestStore.store[:jobs] = []
    end

    def self.flush_jobs
      self.submit!(:all_cached_jobs) if RequestStore.store[:jobs].any?
    end

    def self.stop_caching_jobs
      RequestStore.store[:cache_jobs] = false
    end

    def self.run_with_em(klass, method, *args)
      EventMachine.run do
        self.cache_jobs
        klass.send(method, *args)
        self.flush_jobs
        self.publisher_shutdown
      end
    end

    def self.publisher_shutdown
      EventMachine.add_periodic_timer(1) do
        if RequestStore.store[:jobs] && (RequestStore.store[:jobs].length > 0)
          Rails.logger.info("Waiting for #{RequestStore.store[:jobs].length} publishes to finish")
        else
          Rails.logger.info("Shutting down coney island publisher")
          EventMachine.stop
        end
      end
    end
  end
end

