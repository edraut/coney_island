require 'test_helper'

class PerformerTest < MiniTest::Test
  describe "settings" do
    it 'sets default class-level settings' do
      job = ConeyIsland::Job.new(nil,
        { 'klass' => 'MyPerformer',
          'method_name' => 'set_color',
          'args' => ['carmine'] }
      )
      job.delay.must_equal 1
      job.timeout.must_equal 5
    end
    it 'overrides class-level settings with job-level settings' do
      job = ConeyIsland::Job.new(nil,
        { 'klass' => 'MyPerformer',
          'method_name' => 'set_color',
          'args' => ['carmine'],
          'delay' => 3,
          'timeout' => 10 }
      )
      job.delay.must_equal 3
      job.timeout.must_equal 10
    end
    it 'sets work queue for the class' do
      @exchange = Minitest::Mock.new
      def @exchange.publish(payload,options,&blk)
        ::PerformerTest.messages[:publish_hash] = options
      end
      ConeyIsland::Submitter.stub(:handle_connection, nil) do
        ConeyIsland::Submitter.stub(:exchange, @exchange) do
          ConeyIsland::Submitter.stop_running_inline
          ConeyIsland::Submitter.submit(MyPerformer, :perform, args: [], delay: 0)
        end
      end
      @exchange.verify
      ::PerformerTest.messages[:publish_hash][:routing_key].must_equal "carousels.cyclone"
    end
    it 'overrides the class work queue with the job-level work queue' do
      @exchange = Minitest::Mock.new
      def @exchange.publish(payload,options,&blk)
        ::PerformerTest.messages[:publish_hash] = options
      end
      ConeyIsland::Submitter.stub(:handle_connection, nil) do
        ConeyIsland::Submitter.stub(:exchange, @exchange) do
          ConeyIsland::Submitter.stop_running_inline
          ConeyIsland::Submitter.submit(MyPerformer, :add_to_list, work_queue: 'boardwalk', args: [[]], delay: 0)
        end
      end
      @exchange.verify
      ::PerformerTest.messages[:publish_hash][:routing_key].must_equal "carousels.boardwalk"
    end

    it "inherits settings from a base class" do
      @exchange = Minitest::Mock.new
      def @exchange.publish(payload,options,&blk)
        ::PerformerTest.messages[:publish_hash] = options
      end
      ConeyIsland::Submitter.stub(:handle_connection, nil) do
        ConeyIsland::Submitter.stub(:exchange, @exchange) do
          ConeyIsland::Submitter.stop_running_inline
          ConeyIsland::Submitter.submit(MyInheritedPerformer, :perform, args: [], delay: 0)
        end
      end
      @exchange.verify
      ::PerformerTest.messages[:publish_hash][:routing_key].must_equal "carousels.this-other-queue"
    end
  end

  describe "#get_coney_settings" do
    it "inherits from the defaults" do
      MySingleton.get_coney_settings.must_equal ConeyIsland.default_settings
    end

    it "is inheritable by subclasses" do
      MyInheritedPerformer.get_coney_settings[:work_queue].must_equal 'this-other-queue'
      # These come from the base class
      MyInheritedPerformer.get_coney_settings[:timeout].must_equal 5
      MyInheritedPerformer.get_coney_settings[:delay].must_equal 1
    end
  end

  describe 'async methods' do
    it 'responds to async instance methods' do
      my_performer = MyPerformer.new(7)
      ConeyIsland.run_inline
      my_performer.set_color_async 'sage'
      my_performer.color.must_equal 'sage'
    end
    it 'reponds to async class methods' do
      ConeyIsland.run_inline
      MyPerformer.set_tone_async 'contemplative'
      MyPerformer.tone.must_equal 'contemplative'
    end
    it 'responds to async methods on singletons' do
      my_singleton = MySingleton.new
      ConeyIsland.run_inline
      singleton_mock = Minitest::Mock.new
      singleton_mock.expect :perform, nil, ['interesting stuff']
      MySingleton.stub(:new, singleton_mock) do
        my_singleton.perform_async 'interesting stuff'
      end
      singleton_mock.verify
    end
  end

  describe 'highlander option' do

    before { ConeyIsland.flush_jobs; ConeyIsland.cache_jobs }
    after  { ConeyIsland.flush_jobs; ConeyIsland.stop_caching_jobs }

    it "is understood by performers" do
      5.times { MyHighlander.increment_async }
      assert_equal 1, ConeyIsland.cached_jobs.length
    end
  end
end

class MyHighlander
  include ConeyIsland::Performer

  set_background_defaults highlander: true
  cattr_reader :counter
  @@counter = 0

  def self.increment
    @@counter += 1
  end
end

class MySingleton
  include ConeyIsland::Performer

  def perform(arg)
  end
end

class MyPerformer
  include ConeyIsland::Performer
  set_background_defaults work_queue: 'cyclone', delay: 1, timeout: 5

  def self.instances
    @instances ||= {}
  end

  def self.find(id)
    instances[id]
  end

  def self.set_tone(tone)
    @tone = tone
  end

  def self.tone
    @tone
  end

  def id
    @id
  end

  def initialize(id)
    @id = id
    self.class.instances[id] = self
  end

  def set_color(color)
    @color = color
  end

  def color
    @color
  end
end

class MyInheritedPerformer < MyPerformer
  set_background_defaults work_queue: "this-other-queue"
end
