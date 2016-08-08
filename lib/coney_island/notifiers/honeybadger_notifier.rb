module ConeyIsland
  module Notifiers
    class HoneybadgerNotifier < BaseNotifier
      def self.notify(message, extra_params)
        Honeybadger.notify(message, { context: extra_params })
      rescue NameError => e
        fail ConfigurationError, fail_message(:honeybadger, "Honeybadger")
      end
    end
  end
end
