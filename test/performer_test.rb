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
      capture_publish = proc do |payload,options|
      end
      @exchange = Minitest::Mock.new
      def @exchange.publish(payload,options,&blk)
        ::PerformerTest.messages[:publish_hash] = options
      end
      ConeyIsland::Submitter.stub(:handle_connection, nil) do
        ConeyIsland::Submitter.stub(:exchange, @exchange) do
          ConeyIsland::Submitter.stop_running_inline
          ConeyIsland::Submitter.submit(MyPerformer, :add_to_list, args: [[]])
        end
      end
      @exchange.verify
      ::PerformerTest.messages[:publish_hash][:routing_key].must_equal "carousels.cyclone"
    end
    it 'overrides the class work queue with the job-level work queue' do
      capture_publish = proc do |payload,options|
      end
      @exchange = Minitest::Mock.new
      def @exchange.publish(payload,options,&blk)
        ::PerformerTest.messages[:publish_hash] = options
      end
      ConeyIsland::Submitter.stub(:handle_connection, nil) do
        ConeyIsland::Submitter.stub(:exchange, @exchange) do
          ConeyIsland::Submitter.stop_running_inline
          ConeyIsland::Submitter.submit(MyPerformer, :add_to_list, work_queue: 'boardwalk', args: [[]])
        end
      end
      @exchange.verify
      ::PerformerTest.messages[:publish_hash][:routing_key].must_equal "carousels.boardwalk"
    end
  end

  describe 'async methods' do
    it 'creates async instance methods' do
      my_performer = MyPerformer.new(7)
      ConeyIsland.run_inline
      my_performer.set_color_async 'sage'
      my_performer.color.must_equal 'sage'
    end
    it 'creates async class methods' do
      ConeyIsland.run_inline
      MyPerformer.set_tone_async 'contemplative'
      MyPerformer.tone.must_equal 'contemplative'
    end
  end
end

class MyPerformer
  include ConeyIsland::Performer
  set_background_defaults work_queue: 'cyclone', delay: 1, timeout: 5
  create_class_async_methods :set_tone
  create_instance_async_methods :set_color

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

