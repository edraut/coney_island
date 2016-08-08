module ConeyIsland
  module Notifiers
    class AirbrakeNotifier < BaseNotifier
      def self.notify(message, extra_params = {})
        Airbrake.notify(message, extra_params)
      rescue NameError => e
        fail ConfigurationError, fail_message(:airbrake, "Airbrake")
      end
    end
  end
end
