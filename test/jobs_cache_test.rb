require 'test_helper'

class JobsCacheTest < MiniTest::Test
  describe ConeyIsland::JobsCache do
    before do
      @instance = ConeyIsland::JobsCache.new
    end

    describe :initialize do
      it "assigns RequestStore to the @adapter" do
        assert_equal @instance.instance_variable_get(:@adapter), RequestStore
      end
    end

    describe :caching_jobs? do
      it "returns false by default" do
        assert_equal @instance.caching_jobs?, false
      end
    end # /caching_jobs?

    describe :cache_jobs do
      it "flips caching jobs to true" do
        @instance.cache_jobs
        assert_equal @instance.caching_jobs?, true
      end
    end

    describe :cached_jobs do
      it "returns a Hash" do
        assert_equal @instance.cached_jobs, {}
      end
    end

    describe :stop_caching_jobs do
      it "flips caching jobs to false" do
        @instance.stop_caching_jobs
        assert_equal @instance.caching_jobs?, false
      end
    end

    describe :cache_job do
      it "adds a job to the cache with a uuid (highlander not true)" do
        SecureRandom.stub :uuid, "asdf" do
          args = [String, :to_s, { highlander: false }]
          @instance.cache_job(*args)
          assert_equal @instance.cached_jobs["asdf"], args
        end
      end

      it "adds a job to the cache with an idempotent key (highlander true)" do
        args = [String, :to_s, { highlander: true }]
        @instance.cache_job(*args)
        @instance.cached_jobs.keys.include?("String-to_s").must_equal true
        @instance.cached_jobs["String-to_s"].must_equal args
      end

      it "understand string keys" do
        args = [String, :to_s, { 'highlander' => true }]
        @instance.cache_job(*args)
        @instance.cached_jobs.keys.include?("String-to_s").must_equal true
        @instance.cached_jobs["String-to_s"].must_equal args
      end

    end

  end
end
