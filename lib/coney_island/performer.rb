module ConeyIsland
  module Performer

    def self.included(base)
      base.extend ClassMethods
      # http://apidock.com/rails/Class/class_attribute
      base.class_attribute :coney_island_settings
    end

    def method_missing(method_name, *args)
      method_str = method_name.to_s
      if method_str =~ /.*_async$/
        synchronous_method = method_str.sub(/_async$/, '')
        if self.respond_to?(:id) && self.class.respond_to?(:find)
          ConeyIsland.submit(self.class, synchronous_method, instance_id: self.id, args: args)
        else
          ConeyIsland.submit(self.class, synchronous_method, singleton: true, args: args)
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
      def set_background_defaults(options = {})
        options = options.dup.symbolize_keys.slice(:work_queue, :delay, :timeout)
        self.coney_island_settings = get_coney_settings.merge(options)
      end

      def get_coney_settings
        self.coney_island_settings ||= {}
      end

      protected

      def method_missing(method_name, *args)
        method_str = method_name.to_s
        if method_str =~ /.*_async$/
          synchronous_method = method_str.sub(/_async$/, '')
          ConeyIsland.submit(self, synchronous_method, args: args)
        else
          super
        end
      end

    end

  end
end
