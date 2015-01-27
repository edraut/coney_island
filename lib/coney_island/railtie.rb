require 'coney_island/coney_island_adapter'
module ConeyIsland
  class Railtie < Rails::Railtie
    initializer "coney_island.coney_island_adapter" do
      ActiveJob::QueueAdapters.send :autoload, :ConeyIslandAdapter
    end
  end
end
