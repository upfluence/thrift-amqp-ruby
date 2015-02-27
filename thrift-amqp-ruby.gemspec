# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'thrift/amqp/ruby/version'

Gem::Specification.new do |spec|
  spec.name          = "thrift-amqp-ruby"
  spec.version       = Thrift::Amqp::Ruby::VERSION
  spec.authors       = ["Alexis Montagne"]
  spec.email         = ["alexis.montagne@gmail.com"]
  spec.summary       = %q{Thrift transport layer over AMQP}
  spec.description   = %q{Thrift transport layer over AMQP}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.6"
  spec.add_development_dependency "rake"
  spec.add_dependency "bunny"
  spec.add_dependency "thrift"
end
