module ConeyIsland
  class Worker

    def self.config=(config_hash)
      @config = config_hash.symbolize_keys!
    end

    def self.config
      @config
    end

    def self.initialize_background
      ENV['NEW_RELIC_AGENT_ENABLED'] = 'false'
      ENV['NEWRELIC_ENABLE'] = 'false'
      @ticket = ARGV[0]
      @ticket ||= 'default'

      @log_io = self.config[:log]
      @log = Logger.new(@log_io)

      @instance_config = self.config[:carousels][@ticket]

      @prefetch_count = @instance_config[:prefetch_count] if @instance_config
      @prefetch_count ||= 20

      @worker_count = @instance_config[:worker_count] if @instance_config
      @worker_count ||= 1
      @child_count = @worker_count - 1
      @child_pids = []

      @full_instance_name = @ticket
      @job_attempts = {}

      @log.level = self.config[:log_level]
      @log.info("config: #{self.config}")

      @notifier = "ConeyIsland::Notifiers::#{self.config[:notifier_service]}Notifier".constantize
    end

    def self.start
      @child_count.times do
        child_pid = Process.fork
        unless child_pid
          @log.info("started child for ticket #{@ticket} with pid #{Process.pid}")
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

        ConeyIsland.handle_connection
        
        @log.info("Connecting to AMQP broker. Running #{AMQP::VERSION}")

        #send a heartbeat every 15 seconds to avoid aggresive network configurations that close quiet connections
        heartbeat_exchange = ConeyIsland.channel.fanout('coney_island_heartbeat')
        EventMachine.add_periodic_timer(15) do
          heartbeat_exchange.publish({:instance_name => @ticket})
        end

        ConeyIsland.channel.prefetch @prefetch_count
        @queue = ConeyIsland.channel.queue(@full_instance_name, auto_delete: false, durable: true)
        @queue.bind(ConeyIsland.exchange, routing_key: 'carousels.' + @ticket)
        @queue.subscribe(:ack => true) do |metadata,payload|
          self.handle_incoming_message(metadata,payload)
        end
      end
    end

    def self.handle_incoming_message(metadata,payload)
      begin
        job_id = SecureRandom.uuid
        @job_attempts[job_id] = 1
        args = JSON.parse(payload)
        @log.info ("Starting job #{job_id}: #{args}")
        if args.has_key? 'delay'
          EventMachine.add_timer(args['delay'].to_i) do
            self.handle_job(metadata,args,job_id)
          end
        else
          self.handle_job(metadata,args,job_id)
        end
      rescue Timeout::Error => e
        self.poke_the_badger(e, {code_source: 'ConeyIsland', job_payload: args, reason: 'timeout in subscribe code before calling job method'})
      rescue Exception => e
        self.poke_the_badger(e, {code_source: 'ConeyIsland', job_payload: args})
        @log.error("ConeyIsland code error, not application code:\n#{e.inspect}\nARGS: #{args}")
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
        if @job_attempts.has_key? job_id
          if @job_attempts[job_id] >= 3
            @log.error("Request #{job_id} timed out after #{timeout} seconds, bailing out after 3 attempts")
            self.finalize_job(metadata,job_id)
            self.poke_the_badger(e, {work_queue: @ticket, job_payload: args, reason: 'Bailed out after 3 attempts'})
          else
            @log.error("Request #{job_id} timed out after #{timeout} seconds on attempt number #{@job_attempts[job_id]}, retrying...")
            @job_attempts[job_id] += 1
            self.handle_job(metadata,args,job_id)
          end
        end
      rescue Exception => e
        self.poke_the_badger(e, {work_queue: @ticket, job_payload: args})
        @log.error("Error executing #{class_name}##{method_name} #{job_id} for id #{args['instance_id']} with args #{args}:")
        @log.error(e.message)
        @log.error(e.backtrace.join("\n"))
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
      @log.info("finished job #{job_id}")
      @job_attempts.delete job_id
    end

    def self.poke_the_badger(message, context, attempts = 1)
      begin
        Timeout::timeout(3) do
          @notifier.notify(message, context)
        end
      rescue
        if attempts <= 3
          attempts += 1
          self.poke_the_badger(message, context, attempts)
        end
      end
    end

    def self.shutdown(signal)
      shutdown_time = Time.now
      @child_pids.each do |child_pid|
        Process.kill(signal, child_pid)
      end
      @queue.unsubscribe
      EventMachine.add_periodic_timer(1) do
        if @job_attempts.any?
          @log.info("Waiting for #{@job_attempts.length} requests to finish")
        else
          @log.info("Shutting down coney island #{@ticket}")
          EventMachine.stop
        end
      end
    end

  end
end