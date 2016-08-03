require 'test_helper'

class ConeyIslandTest < MiniTest::Test
  describe "ConeyIsland running jobs" do

    it "runs inline" do
      ConeyIsland.run_inline
      my_array = []
      ConeyIsland.submit(TestModel, :add_to_list, args: [my_array])
      my_array.first.must_equal 'Added one!'
    end

    it "caches jobs" do
      ConeyIsland.run_inline
      my_array = []
      ConeyIsland.flush_jobs
      ConeyIsland.cache_jobs
      ConeyIsland.submit(TestModel, :add_to_list, args: [my_array])
      ConeyIsland::Submitter.cached_jobs.length.must_equal 1
      my_array.length.must_equal 0
      ConeyIsland.flush_jobs
      my_array.first.must_equal 'Added one!'
      ConeyIsland.stop_caching_jobs
    end

  end
end
