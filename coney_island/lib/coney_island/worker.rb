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

    def self.job_attempts
      @job_attempts ||= {}
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
      @child_pids = []

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
      @amqp_parameters
    end

    def self.handle_connection
      if ConeyIsland.single_amqp_connection?
        ConeyIsland.handle_connection(self.log)
        @exchange = ConeyIsland.exchange
        @channel = ConeyIsland.channel
      else
        self.worker_connection
      end
    end

    def self.worker_connection
      @connection ||= AMQP.connect(self.amqp_parameters)
    rescue AMQP::TCPConnectionFailed => e
      @tcp_connection_retries ||= 0
        @tcp_connection_retries += 1
      if @tcp_connection_retries >= 6
        message = "Failed to connect to RabbitMQ 6 times, bailing out"
        self.log.error(message)
        ConeyIsland.poke_the_badger(e, {
          code_source: 'ConeyIsland::Worker.worker_connection',
          reason: message}
        )
      else
        message = "Failed to connecto to RabbitMQ Attempt ##{@tcp_connection_retries} time(s), trying again in 10 seconds..."
        self.log.error(message)
        ConeyIsland.poke_the_badger(e, {
          code_source: 'ConeyIsland::Worker.worker_connection',
          reason: message})
        sleep(10)
        retry
      end
    else
      @channel  ||= AMQP::Channel.new(@connection)
      @exchange = @channel.topic('coney_island')
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
      EventMachine.run do

        Signal.trap('INT') do
          self.shutdown('INT')
        end
        Signal.trap('TERM') do
          self.shutdown('TERM')
        end

        self.handle_connection
        
        self.log.info("Connecting to AMQP broker. Running #{AMQP::VERSION}")

        #send a heartbeat every 15 seconds to avoid aggresive network configurations that close quiet connections
        heartbeat_exchange = self.channel.fanout('coney_island_heartbeat')
        EventMachine.add_periodic_timer(15) do
          heartbeat_exchange.publish({:instance_name => @ticket})
        end

        self.channel.prefetch @prefetch_count
        @queue = self.channel.queue(@full_instance_name, auto_delete: false, durable: true)
        @queue.bind(self.exchange, routing_key: 'carousels.' + @ticket)
        @queue.subscribe(:ack => true) do |metadata,payload|
          self.handle_incoming_message(metadata,payload)
        end
      end
    end

    def self.handle_incoming_message(metadata,payload)
      begin
        job_id = SecureRandom.uuid
        self.job_attempts[job_id] = 1
        args = JSON.parse(payload)
        self.log.info ("Starting job #{job_id}: #{args}")
        if args.has_key? 'delay'
          EventMachine.add_timer(args['delay'].to_i) do
            self.handle_job(metadata,args,job_id)
          end
        else
          self.handle_job(metadata,args,job_id)
        end
      rescue Timeout::Error => e
        ConeyIsland.poke_the_badger(e, {code_source: 'ConeyIsland', job_payload: args, reason: 'timeout in subscribe code before calling job method'})
      rescue Exception => e
        ConeyIsland.poke_the_badger(e, {code_source: 'ConeyIsland', job_payload: args})
        self.log.error("ConeyIsland code error, not application code:\n#{e.inspect}\nARGS: #{args}")
      end
    end

    def self.handle_job(metadata,args,job_id)
      timeout = args['timeout']
      timeout ||= BG_TIMEOUT_SECONDS
      begin
        Timeout::timeout(timeout) do
          self.execute_job_method(args)
        end
      rescue Timeout::Error => e
        if self.job_attempts.has_key? job_id
          if self.job_attempts[job_id] >= 3
            self.log.error("Request #{job_id} timed out after #{timeout} seconds, bailing out after 3 attempts")
            self.finalize_job(metadata,job_id)
            ConeyIsland.poke_the_badger(e, {work_queue: @ticket, job_payload: args, reason: 'Bailed out after 3 attempts'})
          else
            self.log.error("Request #{job_id} timed out after #{timeout} seconds on attempt number #{self.job_attempts[job_id]}, retrying...")
            self.job_attempts[job_id] += 1
            self.handle_job(metadata,args,job_id)
          end
        end
      rescue Exception => e
        ConeyIsland.poke_the_badger(e, {work_queue: @ticket, job_payload: args})
        self.log.error("Error executing #{args['klass']}##{args['method_name']} #{job_id} for id #{args['instance_id']} with args #{args}:")
        self.log.error(e.message)
        self.log.error(e.backtrace.join("\n"))
        self.finalize_job(metadata,job_id)
      else
        self.finalize_job(metadata,job_id)
      end
    end

    def self.execute_job_method(args)
      class_name = args['klass']
      method_name = args['method_name']
      klass = class_name.constantize
      method_args = args['args']
      if args.has_key? 'instance_id'
        instance_id = args['instance_id']
        object = klass.find(instance_id)
      else
        object = klass
      end
      if method_args and method_args.length > 0
        object.send method_name, *method_args
      else
        object.send method_name
      end
    end

    def self.finalize_job(metadata,job_id)
      metadata.ack
      self.log.info("finished job #{job_id}")
      self.job_attempts.delete job_id
    end

    def self.shutdown(signal)
      shutdown_time = Time.now
      @child_pids.each do |child_pid|
        Process.kill(signal, child_pid)
      end
      @queue.unsubscribe
      EventMachine.add_periodic_timer(1) do
        if self.job_attempts.any?
          self.log.info("Waiting for #{self.job_attempts.length} requests to finish")
        else
          self.log.info("Shutting down coney island #{@ticket}")
          EventMachine.stop
        end
      end
    end

  end
end