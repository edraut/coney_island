module ConeyIsland
  module Notifiers
    class HoneybadgerNotifier
      def self.notify(message, extra_params)
        Honeybadger.notify(message, { context: extra_params })
      end
    end
  end
end