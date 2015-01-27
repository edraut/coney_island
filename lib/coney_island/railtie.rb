require 'coney_island/coney_island_adapter'
module ConeyIsland
  class Railtie < Rails::Railtie
    initializer "coney_island.coney_island_adapter" do
      if defined? ActiveJob
        ActiveJob::QueueAdapters.send :autoload, :ConeyIslandAdapter
      end
    end
  end
end
