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
    describe "error handling" do
      it "handles argument errors for jobs" do
        assert_raises(ConeyIsland::JobArgumentError) do
          ConeyIsland::Submitter.publish_job([:not_a_class, :add_to_list, args: [[]]])
        end
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
