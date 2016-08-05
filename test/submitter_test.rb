require 'test_helper'

class SubmitterTest < MiniTest::Test
  describe "ConeyIsland::Submitter" do
    describe "running jobs inline" do
      it "calls the worker directly" do
        @job = Minitest::Mock.new
        @job.expect :handle_job, nil
        ConeyIsland::Job.stub(:new,@job) do
          ConeyIsland::Submitter.run_inline
          ConeyIsland::Submitter.submit(TestModel, :add_to_list, args: [[]])
        end
        @job.verify
      end
    end
    describe "running jobs in the background" do
      it "publishes the job to the message bus" do
        @exchange = Minitest::Mock.new
        @exchange.expect :publish, nil, [String,Hash]
        ConeyIsland::Submitter.stub(:handle_connection, nil) do
          ConeyIsland::Submitter.stub(:exchange, @exchange) do
            ConeyIsland::Submitter.stop_running_inline
            ConeyIsland::Submitter.submit(TestModel, :add_to_list, args: [[]])
          end
        end
        @exchange.verify
      end
    end


    describe :caching_jobs do
      it "caches job for the duration of the block" do
        jobs_cache = ConeyIsland::Submitter.jobs_cache
        def jobs_cache.cache_job(*args)
          SubmitterTest.messages[:it_passes_here] = true
        end
        assert_equal ConeyIsland::Submitter.caching_jobs?, false
        ConeyIsland::Submitter.caching_jobs do
          ConeyIsland.submit String, :inspect
        end
        assert_equal SubmitterTest.messages[:it_passes_here], true
        assert_equal ConeyIsland::Submitter.caching_jobs?, false
      end

      it "flushes the jobs after the block" do
        jobs_cache = ConeyIsland::Submitter.jobs_cache
        def jobs_cache.flush_jobs
          SubmitterTest.messages[:flushed] = true
        end
        ConeyIsland::Submitter.caching_jobs do
          ConeyIsland.submit String, :inspect
        end
        assert_equal SubmitterTest.messages[:flushed], true
      end
    end


    describe "error handling" do
      it "breaks if klass is not a class or a Module" do
        error = assert_raises(ConeyIsland::JobArgumentError) do
          ConeyIsland::Submitter.publish_job([:not_a_class, :method])
        end
        assert_match /to be a Class or Module/, error.message
      end

      it "breaks if the method_name is not a String or a Symbol" do
        error = assert_raises(ConeyIsland::JobArgumentError) do
          ConeyIsland::Submitter.publish_job([Class, 1])
        end
        assert_match /to be a String or a Symbol/, error.message
      end

      it "handles argument errors for jobs" do
        assert_raises(ConeyIsland::JobArgumentError) do
          ConeyIsland::Submitter.publish_job([:not_a_class, :add_to_list, args: [[]]])
        end
      end
      it "retries on TCP connection errors" do
        ConeyIsland.stop_running_inline
        ConeyIsland.tcp_connection_retry_seed = 0
        @fake_channel = Minitest::Mock.new
        @fake_channel.expect :topic, nil, [String]
        @fake_channel.expect :topic, nil, [String]
        force_tcp_error = ->{
          @attempts ||= 0
          @attempts += 1
          if @attempts == 1
            raise Bunny::TCPConnectionFailed.new({host: '127.0.0.1'})
          else
            return true
          end
        }
        ConeyIsland::Submitter.stub(:start_connection,force_tcp_error) do
          ConeyIsland::Submitter.stub(:create_channel, nil) do
            ConeyIsland::Submitter.stub(:channel, @fake_channel) do
              ConeyIsland::Submitter.handle_connection
            end
          end
        end
        @attempts.must_equal 2
      end
    end

    def setup_mock(klass, method, args, expected_work_queue, work_queue=nil)
      exchange = Minitest::Mock.new
      options = { args: args }
      options.merge! work_queue: work_queue if work_queue
      exchange.expect :publish, nil, [String,{routing_key: "carousels.#{expected_work_queue}"}]
      ConeyIsland::Submitter.stub(:handle_connection, nil) do
        ConeyIsland::Submitter.stub(:exchange, exchange) do
          ConeyIsland::Submitter.stop_running_inline
          ConeyIsland::Submitter.submit(klass, method, options)
        end
      end
      exchange
    end

    describe '.submit' do

      it "is aware of default_settings" do
        @exchange = setup_mock TestModel, :add_to_list, [[]], ConeyIsland.default_settings[:work_queue]
        @exchange.verify
      end

      it "overrides defaults if passed in the args" do
        @exchange = setup_mock TestModel, :add_to_list, [[]], 'my-queue', 'my-queue'
        @exchange.verify
      end
    end

    describe "when submitting a performer" do
      it "inherits the settings from the performer set_background_defaults" do
        @exchange = setup_mock DummyPerformer, :perform, nil, 'foo'
        @exchange.verify
      end

      it "still allows overriding the set_background_defaults" do
        @exchange = setup_mock DummyPerformer, :perform, nil, 'bar', 'bar'
        @exchange.verify
      end
    end

  end

end

class DummyPerformer
  include ConeyIsland::Performer
  set_background_defaults work_queue: 'foo'

  def perform; end
end
