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
      if @run_inline
        self.handle_publish(args)
      else
        EventMachine.next_tick do
          self.handle_publish(args)
        end
      end
    end

    def self.handle_publish(args)
      ConeyIsland.handle_connection unless @run_inline
      jobs = (args.first.is_a? Array) ? args : [args]
      jobs.each do |args|
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
            ConeyIsland.exchange.publish((job_args.to_json), routing_key: "carousels.#{work_queue}")
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

    def self.stop_caching_jobs
      RequestStore.store[:cache_jobs] = false
    end

    def self.run_with_em(klass, method, *args)
      EventMachine.run do
        ConeyIsland.cache_jobs
        klass.send(method, *args)
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
  end
end

