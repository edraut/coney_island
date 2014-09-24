require 'test_helper'

class SubmitterTest < MiniTest::Test
  describe "running jobs inline" do
    it "calls the worker directly" do
      @execute_job_method = Minitest::Mock.new
      @execute_job_method.expect :call, nil, [Hash]
      ConeyIsland::Worker.stub(:execute_job_method,@execute_job_method) do
        ConeyIsland::Submitter.run_inline
        ConeyIsland::Submitter.submit(TestModel, :add_to_list, args: [[]])
      end
      @execute_job_method.verify
    end
  end
  describe "running jobs in the background" do
    it "publishes the job to the message bus" do
      @exchange = Minitest::Mock.new
      @exchange.expect :publish, nil, [String,Hash]
      @next_tick = Minitest::Mock.new
      @next_tick.expect :call, nil, []
      EventMachine.stub(:next_tick, @next_tick) do
        ConeyIsland.stub(:handle_connection, nil) do
          ConeyIsland.stub(:exchange, @exchange) do
            ConeyIsland::Submitter.stop_running_inline
            ConeyIsland::Submitter.submit(TestModel, :add_to_list, args: [[]])
          end
        end
      end
      @next_tick.verify
      @exchange.verify
    end
  end
end