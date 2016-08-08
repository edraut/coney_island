module ConeyIsland
  module Notifiers
    class BaseNotifier
      def self.notify(message = "", extra_params = {})
        # NOOP
      end

      protected

      def self.fail_message(notifier_symbol, notifier_class)
        "You have specified #{notifier_symbol} as your notifier, but #{notifier_class} doesn't seem to be installed. Try adding #{notifier_symbol} to your Gemfile."
      end
    end
  end
end
