require 'test_helper'

class ConeyIslandTest < MiniTest::Test
  describe "ConeyIsland running jobs" do

    def force_tcp_error
      lambda do |params|
        @attempts ||= 0
        @attempts += 1
        if @attempts == 1
          raise AMQP::TCPConnectionFailed.new({host: '127.0.0.1'})
        else
          return true
        end
      end
    end

    it "runs inline" do
      ConeyIsland.run_inline
      my_array = []
      ConeyIsland.submit(TestModel, :add_to_list, args: [my_array])
      my_array.first.must_equal 'Added one!'
    end

    it "caches jobs" do
      ConeyIsland.run_inline
      my_array = []
      ConeyIsland.cache_jobs
      ConeyIsland.submit(TestModel, :add_to_list, args: [my_array])
      RequestStore.store[:jobs].length.must_equal 1
      my_array.length.must_equal 0
      ConeyIsland.flush_jobs
      my_array.first.must_equal 'Added one!'
      ConeyIsland.stop_caching_jobs
    end

    it "retries on TCP connection errors" do
      ConeyIsland.stop_running_inline
      ConeyIsland.tcp_connection_retry_interval = 0
      @fake_channel = MiniTest::Mock.new
      @fake_channel.expect :topic, nil, [String]
      AMQP::Channel.stub(:new,@fake_channel) do
        AMQP.stub(:connect, force_tcp_error) do
          ConeyIsland.handle_connection(Logger.new(File.open(File::NULL, "w")))
        end
      end
      @fake_channel.verify
      ConeyIsland.tcp_connection_retries.must_equal 1
    end

  end
end
