require 'bunny'
require 'thrift'
require 'logger'
require 'timeout'

LOGGER = Logger.new(STDOUT)
LOGGER.level = Logger::INFO

module Thrift
  class AMQPServer < BaseServer
    DEFAULT_TIMEOUT = 15_000 # 15s

    def initialize(processor, iprot_factory, oprot_factory = nil, opts = {})
      @processor = processor
      @iprot_factory = iprot_factory
      @oprot_factory = oprot_factory || iprot_factory

      @queue_name = opts[:queue_name]
      @amqp_uri = opts[:amqp_uri]
      @routing_key = opts[:routing_key]
      @exchange_name = opts[:exchange_name]
      @prefetch = (ENV['QOS_SIZE'] || opts[:prefetch]).to_i
      @timeout = opts[:timeout] ? opts[:timeout] * 1000 : DEFAULT_TIMEOUT
      @consumer_tag = opts[:consumer_tag]
      @fetching_disabled = ENV['RABBITMQ_QOS'] == '0'
      @queue_declare_args = opts[:queue_declare_args] || { durable: true }
    end

    def handle(delivery_info, properties, payload)
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
    end

    def serve
      @conn = Bunny.new(@amqp_uri, continuation_timeout: @timeout * 1000)

      @conn.start
      @channel = @conn.create_channel(nil, @prefetch == 0 ? 1 : @prefetch)

      exchange = @channel.direct(@exchange_name)
      queue = @channel.queue(@queue_name, @queue_declare_args)
      queue.bind exchange, routing_key: @routing_key
      @consumer_tag ||= @channel.generate_consumer_tag

      @channel.prefetch @prefetch

      loop do
        if @fetching_disabled
          LOGGER.info("Fetching disabled")
          sleep @timeout
          next
        end

        LOGGER.info("Fetching message from #{@queue_name}")
        queue.subscribe(
          manual_ack: true,
          block: true,
          consumer_tag: @consumer_tag
        ) do |delivery_info, properties, payload|
          begin
            if @timeout
              begin
                Timeout.timeout(@timeout) do
                  handle(delivery_info, properties, payload)
                end
              rescue Timeout::Error
                LOGGER.info("Timeout raised")
              end
            else
              handle delivery_info, properties, payload
            end
          rescue => e
            LOGGER.info("Error happened: #{e}")
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
