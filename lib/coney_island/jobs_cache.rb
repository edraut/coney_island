module ConeyIsland
  class JobsCache
    attr_reader :adapter

    delegate :publish_job, to: Submitter

    def initialize
      @adapter = RequestStore
      self.cached_jobs = {}
      self.caching_jobs = false
    end

    def caching_jobs?
      !! caching_jobs
    end

    def cache_jobs
      self.caching_jobs = true
      self
    end

    def stop_caching_jobs
      self.caching_jobs = false
      self
    end

    def cache_job(*args)
      self.cached_jobs[generate_id(*args)] = args
    end

    def flush_jobs
      while job = self.cached_jobs.shift
        publish_job(job[1], job[0])
      end
    end

    def cached_jobs
      @adapter.store[:jobs]
    end

    protected

    def cached_jobs=(something)
      @adapter.store[:jobs] = something
    end

    def caching_jobs
      @adapter.store[:caching_jobs]
    end

    def caching_jobs=(boolean)
      @adapter.store[:caching_jobs] = boolean
    end

    def generate_id(*args)
      # Duplicate the args so we don't mess with the original
      _args = args.dup
      # Do we have job arguments and idempotent is true?
      if _args.last.is_a?(Hash) && !!_args.pop[:idempotent]
        # We simply generate an id based on the class, method, arguments signature
        # NOTE: Should we carry over the hash args part for this?
        _args.map(&:to_s).join("-")
      else
        # We generate a new id every time
        SecureRandom.uuid
      end
    end

  end
end
