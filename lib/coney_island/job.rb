module ConeyIsland
  class Job
    CONEY_METHODS  = [:logger, :running_inline?, :poke_the_badger, :delay_seed,
      :default_settings, :submit]
    WORKER_METHODS = [:ticket, :running_jobs]

    attr_accessor :delay, :timeout, :method_name, :class_name, :klass, :method_args, :id, :args,
                  :instance_id, :object, :metadata, :attempts, :retry_limit, :retry_on_exception,
                  :initialization_errors, :dont_log

    delegate *CONEY_METHODS,  to: ConeyIsland
    delegate *WORKER_METHODS, to: Worker

    def initialize(metadata, args)
      args.symbolize_keys!
      @args = args
      @id = SecureRandom.uuid
      @dont_log = args[:dont_log]
      logger.info("Starting job #{@id}: #{@args}") unless self.dont_log
      @delay = args[:delay].to_i if args[:delay]
      @timeout = args[:timeout]
      @method_name = args[:method_name]
      @instance_id = args[:instance_id]
      @singleton = args[:singleton]
      @class_name = args[:klass]
      @klass = @class_name.constantize
      @method_args = args[:args]
      @attempts = args[:attempt_count] || 1
      @retry_limit = args[:retry_limit] || 3
      @retry_on_exception = args[:retry_on_exception]

      @metadata = metadata

      if @klass.included_modules.include?(Performer)
        @delay   ||= @klass.get_coney_settings[:delay]
        @timeout ||= @klass.get_coney_settings[:timeout]
      end

      @timeout ||= default_settings[:timeout]

      if @instance_id.present?
        @object = @klass.find(@instance_id)
      elsif @singleton
        @object = @klass.new
      else
        @object = @klass
      end
    rescue StandardError => e
      metadata.ack if !running_inline?
      self.initialization_errors = true
      logger.error "Error initializing with args #{args}: #{e.message}"
      logger.debug e.backtrace.join("\n")
      poke_the_badger e, message: "Error during job initialization, bailing out",
        work_queue: self.ticket, job_payload: args
      logger.info "finished job #{id}"
    end

    def execute_job_method
      if method_args.present? and method_args.length > 0
        object.send method_name, *method_args
      else
        object.send method_name
      end
    end

    def handle_job
      running_jobs << self
      execute_job_method
    rescue StandardError => e
      logger.error("Error executing #{self.class_name}##{self.method_name} #{self.id} for id #{self.instance_id} with args #{self.args}:")
      logger.error(e.message)
      logger.error(e.backtrace.join("\n"))
      if retry_on_exception && (self.attempts < self.retry_limit)
        poke_the_badger(e, {work_queue: self.ticket, job_payload: self.args, attempt_count: self.attempts})
        logger.error("Resubmitting #{self.id} after error on attempt ##{self.attempts}")
        self.attempts += 1
        submit(self.klass, self.method_name, self.resubmit_args)
      else
        poke_the_badger(e, {work_queue: self.ticket, job_payload: self.args})
        logger.error("Bailing out on #{self.id} after error on final attempt ##{self.attempts}:")
      end
    ensure
      finalize_job
    end

    def next_attempt_delay
      delay_seed**(self.attempts - 1)
    end

    def resubmit_args
      args.stringify_keys.select{|key,val| ['timeout','retry_on_exception','retry_limit','args','instance_id'].include? key}.merge(
        'attempt_count' => self.attempts, 'work_queue' => self.ticket, 'delay' => self.next_attempt_delay)
    end

    def finalize_job
      metadata.ack if !running_inline?
      logger.info("finished job #{id}") unless self.dont_log
      running_jobs.delete self
    end

  end
end
