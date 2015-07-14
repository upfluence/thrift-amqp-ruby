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

      loop do
        LOGGER.info("Fetching message from #{@queue_name}")
        queue.subscribe(
          manual_ack: true,
          block: true
        ) do |delivery_info, properties, payload|
          input = StringIO.new payload
          out = StringIO.new
          transport = IOStreamTransport.new input, out
          protocol = @iprot_factory.get_protocol transport

          begin
            @processor.process protocol, protocol

            if out.length > 0
              out.rewind
              @channel.default_exchange.publish(
                out.read(out.length),
                routing_key: properties.reply_to
              )
            end
          rescue => e
            LOGGER.error("Processor failure #{e}")
          end

          @channel.acknowledge(delivery_info.delivery_tag, false)
        end
      end
    rescue Bunny::TCPConnectionFailedForAllHosts, Bunny::ConnectionClosedError
      LOGGER.error("Can't establish the connection")
      sleep 5

      retry
    end
  end
end
