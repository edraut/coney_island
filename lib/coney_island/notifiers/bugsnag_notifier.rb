module ConeyIsland
  module Notifiers
    class BugsnagNotifier < BaseNotifier
      def self.notify(message, extra_params = {})
        Bugsnag.notify(message, meta_data: extra_params)
      rescue NameError => e
        fail ConfigurationError, fail_message(:bugsnag, "Bugsnag")
      end
    end
  end
end
