module ConeyIsland
  module Performer

    def self.included(base)
      base.extend ClassMethods
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

      def create_instance_async_methods(*synchronous_methods)
        synchronous_methods.each do |synchronous_method|
          define_method("#{synchronous_method}_async") do |*args|
            unless self.respond_to? :id
              raise StandardError.new(
                "#{synchronous_method} is not an instance method, ConeyIsland can't async it via :create_instance_async_methods")
            end
            ConeyIsland.submit(self.class, synchronous_method, instance_id: self.id, args: args)
          end
        end
      end

      def create_class_async_methods(*synchronous_methods)
        synchronous_methods.each do |synchronous_method|
          define_singleton_method("#{synchronous_method}_async") do |*args|
            ConeyIsland.submit(self, synchronous_method, args: args)
          end
        end
      end
    end

  end
end
