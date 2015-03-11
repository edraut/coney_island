module ConeyIsland
  class Job
    attr_accessor :delay, :timeout, :method_name, :class_name, :klass, :method_args, :id, :args,
                  :instance_id, :object, :metadata, :attempts, :retry_limit, :retry_on_exception

    def initialize(metadata, args)
      @args = args
      @id = SecureRandom.uuid
      self.log.info ("Starting job #{@id}: #{@args}")
      @delay = args['delay'].to_i if args['delay']
      @timeout = args['timeout']
      @method_name = args['method_name']
      @instance_id = args['instance_id']
      @singleton = args['singleton']
      @class_name = args['klass']
      @klass = @class_name.constantize
      @method_args = args['args']
      @attempts = args['attempt_count'] || 1
      @retry_limit = args['retry_limit'] || 3
      @retry_on_exception = args['retry_on_exception']
      @metadata = metadata
      if @klass.respond_to? :coney_island_settings
        @delay ||= @klass.coney_island_settings[:delay]
        @timeout ||= @klass.coney_island_settings[:timeout]
      end
      @timeout ||= BG_TIMEOUT_SECONDS
      if @instance_id.present?
        @object = @klass.find(@instance_id)
      elsif @singleton
        @object = @klass.new
      else
        @object = @klass
      end
    end

    def ticket
      ConeyIsland::Worker.ticket
    end

    def log
      ConeyIsland::Worker.log
    end

    def execute_job_method
      if method_args.present? and method_args.length > 0
        object.send method_name, *method_args
      else
        object.send method_name
      end
    end

    def handle_job
      Timeout::timeout(timeout) do
        execute_job_method
      end
    rescue Timeout::Error => e
      if self.attempts >= self.retry_limit
        log.error("Request #{self.id} timed out after #{self.timeout} seconds, bailing out after 3 attempts")
        ConeyIsland.poke_the_badger(e, {work_queue: self.ticket, job_payload: self.args, reason: 'Bailed out after 3 attempts'})
      else
        log.error("Request #{self.id} timed out after #{self.timeout} seconds on attempt number #{self.attempts}, retrying...")
        self.attempts += 1
        ConeyIsland.submit(self.klass, self.method_name, self.resubmit_args)
      end
    rescue Exception => e
      log.error("Error executing #{self.class_name}##{self.method_name} #{self.id} for id #{self.instance_id} with args #{self.args}:")
      log.error(e.message)
      log.error(e.backtrace.join("\n"))
      if retry_on_exception && (self.attempts < self.retry_limit)
        self.attempts += 1
        ConeyIsland.poke_the_badger(e, {work_queue: self.ticket, job_payload: self.args, attempt_count: self.attempts})
        ConeyIsland.submit(self.klass, self.method_name, self.resubmit_args)
        log.error("Resubmitting after error on attempt ##{self.attempts}:")
      else
        ConeyIsland.poke_the_badger(e, {work_queue: self.ticket, job_payload: self.args})
        log.error("Bailing out after error on final attempt ##{self.attempts}:")
      end
    ensure
      finalize_job
    end

    def resubmit_args
      args.select{|key,val| ['timeout','retry_on_exception','retry_limit','args','instance_id'].include? key}.merge(
        'attempt_count' => self.attempts, 'work_queue' => self.ticket, 'delay' => ConeyIsland.delay_seed**(self.attempts - 1))
    end

    def finalize_job
      metadata.ack unless ConeyIsland.running_inline?
      log.info("finished job #{id}")
      ConeyIsland::Worker.running_jobs.delete self
    end

  end
end