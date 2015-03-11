module ConeyIsland
  module Performer

    def self.included(base)
      base.extend ClassMethods
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

      def set_background_defaults(work_queue: nil, delay: nil, timeout: nil)
        self.coney_island_settings[:work_queue] = work_queue
        self.coney_island_settings[:delay] = delay
        self.coney_island_settings[:timeout] = timeout
      end

      def coney_island_settings
        @coney_island_settings ||= {}
      end

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
