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
      ConeyIsland::Submitter.jobs_cache.clear
      my_array = []
      ConeyIsland.cache_jobs
      ConeyIsland.submit(TestModel, :add_to_list, args: [my_array])
      ConeyIsland::Submitter.cached_jobs.length.must_equal 1
      my_array.length.must_equal 0
      ConeyIsland.flush_jobs
      my_array.first.must_equal 'Added one!'
      ConeyIsland.stop_caching_jobs
    end

  end

  describe "notifiers" do
    [:bugsnag, :honeybadger, :airbrake].each do |notifier|
      it "accepts #{notifier} as a notifier" do
        ConeyIsland.config = { notifier: notifier }
        assert_equal "ConeyIsland::Notifiers::#{notifier.to_s.titleize}Notifier".constantize, ConeyIsland.notifier
      end

      it "fails nicely when trying to use #{notifier} without its gem" do
        ConeyIsland.config = { notifier: notifier }
        error = assert_raises(ConeyIsland::ConfigurationError) { ConeyIsland.notifier.notify("ayy no", {}) }
        assert_match /Try adding #{notifier} to your Gemfile/, error.message
      end
    end

    it "accepts :none as a notifier" do
      ConeyIsland.config = { notifier: :none }
      assert_equal ConeyIsland::Notifiers::NullNotifier, ConeyIsland.notifier
    end

    it "fails when passing an unknown notifier" do
      ConeyIsland.config = { notifier: :baba_yaga }
      assert_raises(ConeyIsland::ConfigurationError) { ConeyIsland.notifier.notify("ayy lmao") }
    end
  end
end
