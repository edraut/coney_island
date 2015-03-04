require 'test_helper'

class WorkerTest < MiniTest::Test
  describe "ConeyIsland::Worker" do
    describe "handling jobs" do
      before do
        @metadata = MiniTest::Mock.new
        @metadata.expect :ack, nil
      end
      it 'handles incoming messages' do
        @capture_running_jobs = []
        def @capture_running_jobs.delete(item)
        end
        ConeyIsland::Worker.stub :running_jobs, @capture_running_jobs do
          ConeyIsland::Worker.handle_incoming_message(@metadata,
            "{\"klass\":\"TestModel\",\"method_name\":\"add_to_list\",\"args\":[[]]}")
        end
        @capture_running_jobs.first.args['method_name'].must_equal 'add_to_list'
      end
      it 'passes work_queue to the job' do
        ConeyIsland::Worker.ticket = 'test_queue'
        @capture_running_jobs = []
        def @capture_running_jobs.delete(item)
        end
        ConeyIsland::Worker.stub :running_jobs, @capture_running_jobs do
          ConeyIsland::Worker.handle_incoming_message(@metadata,
            "{\"klass\":\"TestModel\",\"method_name\":\"add_to_list\",\"args\":[[]]}")
        end
        @capture_running_jobs.first.ticket.must_equal 'test_queue'
      end
    end
  end
end