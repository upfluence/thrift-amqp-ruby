require 'thrift'
require 'thrift/amqp/server'
require 'thrift/amqp/client'
require 'test'
require 'test_handler'

def run_server
  processor = Test::Processor.new(TestHandler.new)
  prot_factory = Thrift::JsonProtocolFactory.new

  Thrift::AMQPServer.new(
    processor, prot_factory, nil,
    amqp_uri: ENV['RABBITMQ_URL'] || 'amqp://guest:guest@127.0.0.1:5672/%2f',
    routing_key: 'test',
    exchange_name: 'test',
    queue_name: 'test', prefetch: ENV['PREFETCH'] || 1
  ).serve
end

def build_client
  trans = Thrift::AMQPClientTransport.new(
    ENV['RABBITMQ_URL'] || 'amqp://guest:guest@127.0.0.1:5672/%2f',
    'test', 'test'
  )

  prot = Thrift::JsonProtocol.new(trans)

  [Test::Client.new(prot), trans]
end
