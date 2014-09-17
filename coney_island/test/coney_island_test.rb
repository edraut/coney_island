require 'test_helper'

class ConeyIslandTest < MiniTest::Test
  describe "running jobs" do
    it "runs inline" do
      ConeyIsland.run_inline
      my_array = []
      ConeyIsland.submit(TestModel, :add_to_list, args: [my_array])
      my_array.first.must_equal 'Added one!'
    end
    it "caches jobs" do
      ConeyIsland.run_inline
      my_array = []
      ConeyIsland.cache_jobs
      ConeyIsland.submit(TestModel, :add_to_list, args: [my_array])
      RequestStore.store[:jobs].length.must_equal 1
      my_array.length.must_equal 0
      ConeyIsland.flush_jobs
      my_array.first.must_equal 'Added one!'
      ConeyIsland.stop_caching_jobs
    end
    it "runs offline" do
      ConeyIsland.stop_running_inline
      @exchange = Minitest::Mock.new
      ConeyIsland.exchange = @exchange
      @exchange.expect :publish, nil, [Array]
      ConeyIsland.submit(TestModel, :add_to_list, args: [[]])
      @exchange.verify
    end
  end
end
