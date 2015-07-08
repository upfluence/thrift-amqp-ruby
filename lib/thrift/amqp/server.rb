require 'bunny'
require 'thrift'
require 'logger'

LOGGER = Logger.new(STDOUT)
LOGGER.level = Logger::INFO

module Thrift
  class AMQPServer < BaseServer
    def initialize(processor, iprot_factory, oprot_factory = nil, opts = {})
      @processor = processor
      @iprot_factory = iprot_factory
      @oprot_factory = oprot_factory || iprot_factory

      @queue_name = opts[:queue_name]
      @amqp_uri = opts[:amqp_uri]
      @routing_key = opts[:routing_key]
      @exchange_name = opts[:exchange_name]
      @prefetch = opts[:prefetch]
    end

    def serve
      @conn = Bunny.new(@amqp_uri)

      @conn.start
      @channel = @conn.create_channel

      exchange = @channel.direct(@exchange_name)
      queue = @channel.queue(@queue_name)
      queue.bind exchange, routing_key: @routing_key

      @channel.prefetch @prefetch

      queue.subscribe(
        manual_ack: true,
        block: true
      ) do |delivery_info, _properties, payload|
        trans = MemoryBufferTransport.new(payload)
        iprot = @iprot_factory.get_protocol(trans)

        begin
          @processor.process(iprot, nil)
        rescue => e
          LOGGER.error("Processor failure #{e}")
        end
        @channel.acknowledge(delivery_info.delivery_tag, false)
      end
    end
  end
end
