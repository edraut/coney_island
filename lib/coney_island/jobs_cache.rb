module ConeyIsland
  # Handles job caching hijinks. This is especially useful for Rails apps where
  # you can set Coney to cache jobs at the beginning of the request and flush
  # them once you the request is served. Most methods are exposed by ConeyIsland
  # so you'd just use ConeyIsland.cache_jobs, ConeyIsland.flush_jobs.
  class JobsCache
    delegate :submit!, to: Submitter

    def initialize
      @adapter = RequestStore
    end

    # Are we caching jobs?
    def caching_jobs?
      !! is_caching_jobs
    end

    # Start caching jobs
    def cache_jobs
      self.is_caching_jobs = true
      self
    end

    # Stop caching jobs
    def stop_caching_jobs
      self.is_caching_jobs = false
      self
    end

    # Caches jobs for the duration of the block, flushes them at the end.
    def caching_jobs(&blk)
      _was_caching = caching_jobs?
      cache_jobs
      blk.call
      flush_jobs
      self.is_caching_jobs = _was_caching
      self
    end

    # Cache a job with the given args
    def cache_job(*args)
      self.cached_jobs[generate_id(*args)] = args
      self
    end

    # Publish all the cached jobs
    def flush_jobs
      # Get all the jobs, one at a time, pulling from the list
      while job = self.cached_jobs.shift
        # Map the array to the right things
        job_id, args = *job
        # Submit! takes care of rescuing, error logging, etc and never caches
        submit! args, job_id
      end
      self
    end

    # List of the currently cached jobs, anxiously waiting to be flushed
    def cached_jobs
      @adapter.store[:jobs] ||= {}
    end

    def clear
      self.is_caching_jobs = false
      self.cached_jobs  = {}
    end

    protected

    def cached_jobs=(something)
      @adapter.store[:jobs] = something
    end

    def is_caching_jobs
      @adapter.store[:caching_jobs]
    end

    def is_caching_jobs=(boolean)
      @adapter.store[:caching_jobs] = boolean
    end

    def generate_id(*args)
      # Duplicate the args so we don't mess with the original
      _args = args.dup
      # Do we have job arguments and highlander is true?
      if _args.last.is_a?(Hash) && !!ActiveSupport::HashWithIndifferentAccess.new(_args.pop)[:highlander]
        # We simply generate an id based on the class, method, arguments signature
        _args.map(&:to_s).join("-")
      else
        # We generate a new id every time
        SecureRandom.uuid
      end
    end

  end
end
