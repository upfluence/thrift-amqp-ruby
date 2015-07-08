require 'thread'
require 'thrift'
require 'bunny'
require 'stringio'
require 'timeout'
require 'uuidtools'

module Thrift
  class AMQPClientTransport < BaseTransport
    def initialize(amqp_uri, exchange_name, routing_key)
      @outbuf = Bytes.empty_byte_buffer
      @inbuf = Bytes.empty_byte_buffer
      @conn = Bunny.new(amqp_uri)
      @queue = Queue.new

      @exchange_name, @routing_key = exchange_name, routing_key
    end

    def open
      unless @channel
        @conn.start
        @channel = @conn.create_channel
        @reply_queue = @channel.queue("", auto_delete: true, exclusive: true)

        @reply_queue.subscribe(block: false, ack: true) do |delivery_info, properties, payload|
          @queue << true
          @inbuf << payload
        end
      end
    end


    def close
      if open?
        @reply_queue.delete
        @channel.close
      end
    end

    def open?
      @channel && @channel.open?
    end

    def read(sz)
      buf = @queue.pop
      @inbuf.read sz
    end

    def write(buf)
      @outbuf << Bytes.force_binary_encoding(buf)
    end

    def flush
      @service_exchange.publish(
        @outbuf,
        routing_key: @routing_key,
        correlation_id: self.generate_uuid,
        reply_to: @reply_queue.name
      )


      @outbuf = Bytes.empty_byte_buffer
    end

    prtected

    def generate_uuid
      UUIDTools::UUID.timestamp_create.to_s
    end
  end
end
