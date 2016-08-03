module ConeyIsland
  module Notifiers
    class BugsnagNotifier
      def self.notify(message, extra_params)
        Bugsnag.notify(message, meta_data: { extra_params })
      end
    end
  end
end
