module ConeyIsland
  module Notifiers
    class AirbrakeNotifier
      def self.notify(message, extra_params)
        Airbrake.notify(message, extra_params)
      end
    end
  end
end