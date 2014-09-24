# Configure Rails Environment
ENV["RAILS_ENV"] = "test"

require File.expand_path("../dummy/config/environment.rb",  __FILE__)
require "rails/test_help"
require 'minitest/unit'
require 'minitest/autorun'
require 'minitest/pride'
require 'request_store'
require 'amqp'

Rails.backtrace_cleaner.remove_silencers!
# Load support files
Dir["#{File.dirname(__FILE__)}/support/**/*.rb"].each { |f| require f }

# Load fixtures from the engine
if ActiveSupport::TestCase.method_defined?(:fixture_path=)
  ActiveSupport::TestCase.fixture_path = File.expand_path("../fixtures", __FILE__)
end

class TestModel

  def self.add_to_list(array)
    array.push "Added one!"
  end

  def self.take_too_long
    sleep(10)
  end

  def self.throw_an_error
    raise 'It broke!'
  end

  def self.instances
    @instances ||= {}
  end

  def self.find(id)
    self.instances[id]
  end

  def initialize(params)
    TestModel.instances[params['id']] = self
  end

  def set_color(color)
    @color = (color)
  end

  def color
    @color
  end
end
