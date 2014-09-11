module ConeyIsland
  BG_TIMEOUT_SECONDS = 30

  def self.run_inline
    @run_inline = true
  end

  def self.notification_service=(service_name)
    @notifier = "ConeyIsland::Notifiers::#{service_name}Notifier".constantize
  end

  def self.amqp_connection
    @connection
  end

  def self.amqp_parameters=(params)
    @amqp_parameters = params
    @amqp_parameters ||= self.config['amqp_connection']
  end

  def self.amqp_parameters
    @amqp_parameters
  end

  def self.handle_connection
    @connection ||= AMQP.connect(self.amqp_parameters)
    @channel  ||= AMQP::Channel.new(@connection)
    @exchange ||= @channel.topic('coney_island')
  end

  def self.exchange
    @exchange
  end

  def self.channel
    @channel
  end

  def self.config
    if(File.exists?(File.join(Rails.root,"config","coney_island.yml")))
      @config = Psych.load(File.read(File.join(Rails.root,"config","coney_island.yml")))
      @config = @config[Rails.env]
    end
  end

  #BEGIN web functionality

  def self.submit(*args)
    if RequestStore.store[:cache_jobs]
      RequestStore.store[:jobs].push args
    else
      self.submit!(args)
    end
  end

  def self.submit!(args)
    if @run_inline
      self.handle_publish(args)
    else
      EventMachine.next_tick do
        self.handle_publish(args)
      end
    end
  end

  def self.handle_publish(args)
    self.handle_connection unless @run_inline
    jobs = (args.first.is_a? Array) ? args : [args]
    jobs.each do |args|
      if (args.first.is_a? Class or args.first.is_a? Module) and (args[1].is_a? String or args[1].is_a? Symbol) and args.last.is_a? Hash and 3 == args.length
        klass = args.shift
        klass = klass.name unless @run_inline
        method_name = args.shift
        job_args = args.shift
        job_args ||= {}
        job_args['klass'] = klass
        job_args['method_name'] = method_name
        if @run_inline
          job_args.stringify_keys!
          method_args = job_args['args']
          if job_args.has_key? 'instance_id'
            instance_id = job_args.delete 'instance_id'
            object = klass.find(instance_id)
          else
            object = klass
          end
          if method_args && (method_args.length > 0)
            object.send method_name, *method_args
          else
            object.send method_name
          end
        else
          work_queue = job_args.delete :work_queue
          work_queue ||= 'default'
          self.exchange.publish((job_args.to_json), routing_key: "carousels.#{work_queue}")
        end
      end
      RequestStore.store[:completed_jobs] ||= 0
      RequestStore.store[:completed_jobs] += 1
    end
  end

  def self.cache_jobs
    RequestStore.store[:cache_jobs] = true
    RequestStore.store[:jobs] = []
  end

  def self.flush_jobs
    jobs = RequestStore.store[:jobs].dup
    self.submit!(jobs) if jobs.any?
    RequestStore.store[:jobs] = []
  end

  def self.run_with_em(klass, method, *args)
    EventMachine.run do
      ConeyIsland.cache_jobs
      ConeyIsland.submit(klass, method, *args)
      ConeyIsland.flush_jobs
      ConeyIsland.publisher_shutdown
    end
  end

  def self.publisher_shutdown
    EventMachine.add_periodic_timer(1) do
      if RequestStore.store[:jobs] && (RequestStore.store[:jobs].length > RequestStore.store[:completed_jobs])
        Rails.logger.info("Waiting for #{RequestStore.store[:jobs].length - RequestStore.store[:completed_jobs]} publishes to finish")
      else
        Rails.logger.info("Shutting down coney island publisher")
        EventMachine.stop
      end
    end
  end

  # BEGIN background functionality

  def self.initialize_background
    ENV['NEW_RELIC_AGENT_ENABLED'] = 'false'
    ENV['NEWRELIC_ENABLE'] = 'false'
    @ticket = ARGV[0]

    #TODO: set an env variable or constant that can be checked by pubnub to decide sync or not
    @log_io = self.config['log'].constantize rescue nil
    @log_io ||= self.config['log']
    @log = Logger.new(@log_io)

    @ticket ||= 'default'

    @instance_config = self.config['carousels'][@ticket]

    @prefetch_count = @instance_config['prefetch_count'] if @instance_config
    @prefetch_count ||= 20

    @worker_count = @instance_config['worker_count'] if @instance_config
    @worker_count ||= 1
    @child_count = @worker_count - 1
    @child_pids = []

    @full_instance_name = @ticket
    @job_attempts = {}

    @log.level = @config['log_level']
    @log.info("config: #{self.config}")

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

  def self.handle_job(metadata,args,job_id)
    class_name = args['klass']
    method_name = args['method_name']
    klass = class_name.constantize
    method_args = args['args']
    timeout = args['timeout']
    timeout ||= BG_TIMEOUT_SECONDS
    begin
      Timeout::timeout(timeout) do
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

  def self.finalize_job(metadata,job_id)
    metadata.ack
    @log.info("finished job #{job_id}")
    @job_attempts.delete job_id
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

      self.handle_connection
      @log.info("Connecting to AMQP broker. Running #{AMQP::VERSION}")

      #send a heartbeat every 15 seconds to avoid aggresive network configurations that close quiet connections
      heartbeat_exchange = self.channel.fanout('coney_island_heartbeat')
      EventMachine.add_periodic_timer(15) do
        heartbeat_exchange.publish({:instance_name => @ticket})
      end

      self.channel.prefetch @prefetch_count
      @queue = self.channel.queue(@full_instance_name, auto_delete: false, durable: true)
      @queue.bind(self.exchange, routing_key: 'carousels.' + @ticket)
      @queue.subscribe(:ack => true) do |metadata,payload|
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
    end
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
end
require 'coney_island/notifiers/honeybadger_notifier'

