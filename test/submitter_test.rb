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
  end
end