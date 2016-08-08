$:.push File.expand_path("../lib", __FILE__)

# Maintain your gem's version:
require "coney_island/version"

# Describe your gem and declare its dependencies:
Gem::Specification.new do |s|
  s.name        = "coney_island"
  s.version     = ConeyIsland::VERSION
  s.authors     = ["Eric Draut","Adam Bialek","Leonardo Bighetti"]
  s.email       = ["edraut@gmail.com"]
  s.homepage    = "https://github.com/edraut/coney_island/wiki"
  s.summary     = "Want guaranteed delivery between your queue and your workers using ACKs? How about load-balancing? Throw in all the features other background worker systems offer and you must have a ticket to ride at Coney Island."
  s.description = "An industrial-strength background worker system for rails using RabbitMQ."
  s.license     = "MIT"

  s.require_paths = ["lib"]
  s.executables = ['coney_island']
  s.files = Dir["{app,config,db,lib}/**/*", "MIT-LICENSE", "Rakefile", "README.rdoc","bin/*"]
  s.test_files = Dir["test/**/*"]

  s.add_dependency "rails", [">= 4.0.1", "<5"]
  s.add_dependency "amqp", [">= 1.5.0", "<2"]
  s.add_dependency "bunny", "< 2.0"
  s.add_dependency "request_store", [">= 1.0.8", "<2"]
  s.add_dependency "eventmachine", "<2"

  s.add_development_dependency "sqlite3", '~> 0'
  s.add_development_dependency "minitest", '~> 0'
  s.add_development_dependency "mocha", '~> 0'
end
