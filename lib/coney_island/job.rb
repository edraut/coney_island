module ConeyIsland
  class Job
    attr_accessor :delay, :timeout, :method_name, :class_name, :klass, :method_args, :id, :args,
                  :instance_id, :object, :metadata, :attempts, :retry_limit, :retry_on_exception,
                  :initialization_errors, :dont_log

    def initialize(metadata, args)
      @args = args
      @id = SecureRandom.uuid
      @dont_log = args['dont_log']
      self.log.info ("Starting job #{@id}: #{@args}") unless self.dont_log
      @delay = args['delay'].to_i if args['delay']
      @timeout = args['timeout']
      @method_name = args['method_name']
      @instance_id = args['instance_id']
      @singleton = args['singleton']
      @class_name = args['klass']
      @klass = @class_name.constantize
      @method_args = args['args']
      # Symbolize hash keys for consistency and for keyword arguments
      @method_args.each { |v| v.symbolize_keys! if v.is_a?(Hash) } if !!@method_args
      @attempts = args['attempt_count'] || 1
      @retry_limit = args['retry_limit'] || 3
      @retry_on_exception = args['retry_on_exception']

      @metadata = metadata

      if @klass.included_modules.include?(Performer)
        @delay   ||= @klass.get_coney_settings[:delay]
        @timeout ||= @klass.get_coney_settings[:timeout]
      end

      @timeout ||= ConeyIsland.default_settings[:timeout]

      if @instance_id.present?
        @object = @klass.find(@instance_id)
      elsif @singleton
        @object = @klass.new
      else
        @object = @klass
      end
    rescue StandardError => e
      metadata.ack if !ConeyIsland.running_inline?
      self.initialization_errors = true
      log.error("Error initializing with args #{args}:")
      log.error(e.message)
      log.error(e.backtrace.join("\n"))
      ConeyIsland.poke_the_badger(e, {message: "Error during job initialization, bailing out", work_queue: self.ticket, job_payload: args})
      log.info("finished job #{id}")
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
      ConeyIsland::Worker.running_jobs << self
      execute_job_method
    rescue StandardError => e
      log.error("Error executing #{self.class_name}##{self.method_name} #{self.id} for id #{self.instance_id} with args #{self.args}:")
      log.error(e.message)
      log.error(e.backtrace.join("\n"))
      if retry_on_exception && (self.attempts < self.retry_limit)
        ConeyIsland.poke_the_badger(e, {work_queue: self.ticket, job_payload: self.args, attempt_count: self.attempts})
        log.error("Resubmitting #{self.id} after error on attempt ##{self.attempts}")
        self.attempts += 1
        ConeyIsland.submit(self.klass, self.method_name, self.resubmit_args)
      else
        ConeyIsland.poke_the_badger(e, {work_queue: self.ticket, job_payload: self.args})
        log.error("Bailing out on #{self.id} after error on final attempt ##{self.attempts}:")
      end
    ensure
      finalize_job
    end

    def next_attempt_delay
      ConeyIsland.delay_seed**(self.attempts - 1)
    end

    def resubmit_args
      args.select{|key,val| ['timeout','retry_on_exception','retry_limit','args','instance_id'].include? key}.merge(
        'attempt_count' => self.attempts, 'work_queue' => self.ticket, 'delay' => self.next_attempt_delay)
    end

    def finalize_job
      metadata.ack if !ConeyIsland.running_inline?
      log.info("finished job #{id}") unless self.dont_log
      ConeyIsland::Worker.running_jobs.delete self
    end

  end
end
