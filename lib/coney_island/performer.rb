module ConeyIsland
  module Performer

    def self.included(base)
      base.extend ClassMethods
      delegate :get_coney_settings, to: :class
      # http://apidock.com/rails/Class/class_attribute
      base.class_attribute :coney_island_settings
    end


    def method_missing(method_name, *args)
      method_str = method_name.to_s
      if method_str =~ /.*_async$/
        synchronous_method = method_str.sub(/_async$/, '')
        if self.respond_to?(:id) && self.class.respond_to?(:find)
          ConeyIsland.submit(self.class, synchronous_method, instance_id: self.id, args: args, highlander: get_coney_settings[:highlander])
        else
          ConeyIsland.submit(self.class, synchronous_method, singleton: true, args: args, highlander: get_coney_settings[:highlander])
        end
      else
        super
      end
    end

    module ClassMethods

      # Sets inheritable class defaults for ConeyIsland.
      # Valid options:
      #   :work_queue - use a named queue for this class.
      #   :delay - Delay execution of the job on the worker. The delay value is
      #     a number of seconds.
      #   :timeout - Timeout the job with retry. The timeout value is a number
      #     of seconds. By default ConeyIsland will retry 3 times before bailing
      #     out.
      #   :highlander - There can only be one job with the same arguments per
      # request lifecycle. This makes it so that even if you enqueue the same
      # job with the same arguments twice, it will only fire once.
      # Only makes sense when caching jobs (like in a Rails app where you can
      # cache jobs and flush them all at once after the end of the request)
      def set_background_defaults(options = {})
        options = options.dup.symbolize_keys.slice(:work_queue, :delay, :timeout, :highlander)
        self.coney_island_settings = get_coney_settings.merge(options)
      end

      def get_coney_settings
        self.coney_island_settings ||= ConeyIsland.default_settings
      end

      protected

      def method_missing(method_name, *args)
        method_str = method_name.to_s
        if method_str =~ /.*_async$/
          synchronous_method = method_str.sub(/_async$/, '')
          ConeyIsland.submit(self, synchronous_method, args: args, highlander: get_coney_settings[:highlander])
        else
          super
        end
      end

    end

  end
end
