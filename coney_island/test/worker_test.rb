require 'test_helper'

class WorkerTest < MiniTest::Test
  describe "ConeyIsland::Worker" do
    describe "handling jobs" do
      before do
        @metadata = MiniTest::Mock.new
        @metadata.expect :ack, nil
      end
      it "handles timeouts with 3 retries before bailing out" do
        ConeyIsland::Worker.job_attempts['my_job_id'] = 1
        ConeyIsland::Worker.job_attempts.stub(:delete,nil) do
          ConeyIsland::Worker.handle_job(@metadata,{
            'klass' => 'TestModel',
            'method_name' => :take_too_long,
            'timeout' => 0.0001
            },'my_job_id')
        end
        ConeyIsland::Worker.job_attempts['my_job_id'].must_equal 3
      end
      it "sends other exeptions to a notification service" do
        @poke_the_badger = MiniTest::Mock.new
        @poke_the_badger.expect :call, nil, [Exception,Hash]
        ConeyIsland.stub(:poke_the_badger,@poke_the_badger) do
          ConeyIsland::Worker.handle_job(@metadata,{
            'klass' => 'TestModel',
            'method_name' => :throw_an_error
            },'my_job_id')
        end
        @poke_the_badger.verify
      end
      it "calls find on the submitted class if an instance_id is present" do
        TestModel.new('id' => 'my_id')
        ConeyIsland::Worker.handle_job(@metadata,{
          'klass' => 'TestModel',
          'method_name' => :set_color,
          'instance_id' => 'my_id',
          'args' => ['green']
          },'my_job_id')
        my_thing = TestModel.find('my_id')
        my_thing.color.must_equal 'green' 
      end
      it "acknowledges job completion to the message bus" do
        ConeyIsland::Worker.handle_job(@metadata,
          { 'klass' => 'TestModel',
            'method_name' => :add_to_list,
            'args' => [[]]},
          'my_job_id')
        @metadata.verify
      end
    end
  end
end