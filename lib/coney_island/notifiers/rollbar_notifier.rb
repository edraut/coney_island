module ConeyIsland
  module Notifiers
    class RollbarNotifier < BaseNotifier
      def self.notify(message, extra_params = {})
        dont_raise = RuntimeError.new(message)
        dont_raise.set_backtrace(caller)
        Rollbar.error(dont_raise, app_data: extra_params)
      rescue NameError => e
        fail ConfigurationError, fail_message(:bugsnag, "Bugsnag")
      end
    end
  end
end
