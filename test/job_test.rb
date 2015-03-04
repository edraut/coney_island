require 'test_helper'

class JobTest < MiniTest::Test
  describe "ConeyIsland::Job" do
    describe "handling a job" do
      before do
        @metadata = MiniTest::Mock.new
        @metadata.expect :ack, nil
      end

      it "retries on timeout with correct initial attempt_count and delay" do
        job = ConeyIsland::Job.new(@metadata,
          { 'klass' => 'TestModel',
            'method_name' => :take_too_long,
            'timeout' => 0.0001 }
        )
        capture_submissions = lambda { |klass,method_name,options|
          ::JobTest.messages[:job_options] ||= []
          ::JobTest.messages[:job_options] << options
        }
        ConeyIsland.stub(:submit, capture_submissions) do
          job.handle_job
        end
        ::JobTest.messages[:job_options].last['attempt_count'].must_equal 2
        ::JobTest.messages[:job_options].last['delay'].must_equal 2
      end

      it "retries on timeout with correct subsequent attempt_count and delay" do
        job = ConeyIsland::Job.new(@metadata,
          { 'klass' => 'TestModel',
            'method_name' => :take_too_long,
            'timeout' => 0.0001,
            'attempt_count' => 2,
            'delay' => 2 }
        )
        capture_submissions = lambda { |klass,method_name,options|
          ::JobTest.messages[:job_options] ||= []
          ::JobTest.messages[:job_options] << options
        }
        ConeyIsland.stub(:submit, capture_submissions) do
          job.handle_job
        end
        ::JobTest.messages[:job_options].last['attempt_count'].must_equal 3
        ::JobTest.messages[:job_options].last['delay'].must_equal 4
      end

      it "bails out on timeout if retry limit reached" do
        job = ConeyIsland::Job.new(@metadata,
          { 'klass' => 'TestModel',
            'method_name' => :take_too_long,
            'timeout' => 0.0001,
            'attempt_count' => 3,
            'delay' => 2 }
        )
        capture_submissions = lambda { |klass,method_name,options|
          fail "Should not have called ConeyIsland.submit, should have bailed out instead"
        }
        ConeyIsland.stub(:submit, capture_submissions) do
          job.handle_job
        end
        @metadata.verify #we ack the message bus when we bail out
      end

      it "retries on exception with correct initial attempt_count and delay if retry_on_exception set" do
        job = ConeyIsland::Job.new(@metadata,
          { 'klass' => 'TestModel',
            'method_name' => :throw_an_error,
            'retry_on_exception' => true }
        )
        capture_submissions = lambda { |klass,method_name,options|
          ::JobTest.messages[:job_options] ||= []
          ::JobTest.messages[:job_options] << options
        }
        ConeyIsland.stub(:submit, capture_submissions) do
          job.handle_job
        end
        ::JobTest.messages[:job_options].last['attempt_count'].must_equal 2
        ::JobTest.messages[:job_options].last['delay'].must_equal 2
      end

      it "bails out on exception if retry_on_exception set and retry_limit reached" do
        job = ConeyIsland::Job.new(@metadata,
          { 'klass' => 'TestModel',
            'method_name' => :throw_an_error,
            'retry_on_exception' => true,
            'retry_limit' => 2,
            'attempt_count' => 2}
        )
        capture_submissions = lambda { |klass,method_name,options|
          fail "Should not have called ConeyIsland.submit, should have bailed out instead"
        }
        ConeyIsland.stub(:submit, capture_submissions) do
          job.handle_job
        end
        @metadata.verify #we ack the message bus when we bail out
      end

      it "bails out on exception if retry_on_exception not set" do
        job = ConeyIsland::Job.new(@metadata,
          { 'klass' => 'TestModel',
            'method_name' => :throw_an_error }
        )
        capture_submissions = lambda { |klass,method_name,options|
          fail "Should not have called ConeyIsland.submit, should have bailed out instead"
        }
        ConeyIsland.stub(:submit, capture_submissions) do
          job.handle_job
        end
        @metadata.verify #we ack the message bus when we bail out
      end

      it "sends exeptions to a notification service when bailing out" do
        @poke_the_badger = MiniTest::Mock.new
        @poke_the_badger.expect :call, nil, [Exception,Hash]
        job = ConeyIsland::Job.new(@metadata,
          { 'klass' => 'TestModel',
            'method_name' => :throw_an_error }
        )
        ConeyIsland.stub(:poke_the_badger,@poke_the_badger) do
          job.handle_job
        end
        @poke_the_badger.verify
      end

      it "calls find on the submitted class if an instance_id is present" do
        TestModel.new('id' => 'my_id')
        job = ConeyIsland::Job.new(@metadata,{
          'klass' => 'TestModel',
          'method_name' => :set_color,
          'instance_id' => 'my_id',
          'args' => ['green']
          })
        job.handle_job
        my_thing = TestModel.find('my_id')
        my_thing.color.must_equal 'green'
      end

      it "acknowledges job completion to the message bus" do
        ConeyIsland.stop_running_inline
        job = ConeyIsland::Job.new(@metadata,
          { 'klass' => 'TestModel',
            'method_name' => :add_to_list,
            'args' => [[]]})
        job.handle_job
        @metadata.verify
        ConeyIsland::Worker.running_jobs.must_be :empty?
      end

    end
  end
end