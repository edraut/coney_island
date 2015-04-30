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

      def set_background_defaults(work_queue: nil, delay: nil, timeout: nil)
        # This works as intended, subclasses get their own specific configurations
        # if they pass any and inherit config from base classes but we should
        # treat these defaults better or whatever isn't passed gets merged
        # as nil here.
        self.coney_island_settings = get_coney_settings.merge \
          work_queue: work_queue, delay: delay, timeout: timeout
      end

      def get_coney_settings
        self.coney_island_settings ||= {}
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
