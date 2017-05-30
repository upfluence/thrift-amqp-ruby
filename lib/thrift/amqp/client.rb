require 'thread'
require 'thrift'
require 'bunny'
require 'stringio'
require 'timeout'
require 'securerandom'

module Thrift
  class AMQPClientTransport < BaseTransport
    def initialize(amqp_uri, exchange_name, routing_key)
      @outbuf = Bytes.empty_byte_buffer
      @inbuf = StringIO.new
      @conn = Bunny.new(amqp_uri)
      @queue = Queue.new

      @exchange_name, @routing_key = exchange_name, routing_key
    end

    def open
      return if open?

      @conn.start
      @channel = @conn.create_channel
      @service_exchange = @channel.exchange(@exchange_name)
      @reply_queue = @channel.queue('', auto_delete: true, exclusive: true)

      @reply_queue.subscribe(block: false, manual_ack: true) do |delivery_info, properties, payload|
        @inbuf.write payload
        @queue << true
        @channel.acknowledge(delivery_info.delivery_tag, false)
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
      @queue.pop if @inbuf.eof?
      @inbuf.read(sz)
    end

    def write(buf)
      @outbuf << Bytes.force_binary_encoding(buf)
    end

    def flush
      open unless open?

      @service_exchange.publish(
        @outbuf,
        routing_key: @routing_key,
        correlation_id: self.generate_uuid,
        reply_to: @reply_queue.name
      )

      @outbuf = Bytes.empty_byte_buffer
    end

    protected

    def generate_uuid
       SecureRandom.hex(13)
    end
  end
end
