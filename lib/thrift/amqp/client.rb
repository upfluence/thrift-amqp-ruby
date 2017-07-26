require 'thread'
require 'thrift'
require 'bunny'
require 'stringio'
require 'timeout'
require 'securerandom'

module Thrift
  class AMQPClientTransport < BaseTransport
    class << self
      def from_channel(channel, exchange_name, routing_key)
        new(nil, exchange_name, routing_key, channel: channel)
      end
    end

    def initialize(amqp_uri, exchange_name, routing_key, opts = {})
      @outbuf = Bytes.empty_byte_buffer
      @inbuf_r, @inbuf_w = IO.pipe(binmode: true)
      @inbuf_w.set_encoding('binary')

      if opts[:channel]
        @channel = opts[:channel]
      else
        @conn = Bunny.new(amqp_uri)
      end

      @opened = false
      @handle_conn_lifecycle = opts[:channel].nil?
      @exchange_name = exchange_name
      @routing_key = routing_key
    end

    def open
      return if open?

      if @channel.nil? || !@channel.open?
        unless @conn
          raise TransportException.new(
            TransportException::NOT_OPEN, 'channel cosed'
          )
        end

        @conn.start
        @channel = @conn.create_channel
      end

      @service_exchange = @channel.exchange(@exchange_name)
      @reply_queue = @channel.queue('', auto_delete: true, exclusive: true)

      @reply_queue.subscribe(block: false, manual_ack: true) do |delivery_info, properties, payload|
        @inbuf_w << Bytes.force_binary_encoding(payload)
        @channel.acknowledge(delivery_info.delivery_tag, false)
      end
      @opened = true
    end


    def close
      if open?
        @reply_queue.delete
        @channel.close if @handle_conn_lifecycle
        @opened = false
      end
    end

    def open?
      @opened && @channel && @channel.open?
    end

    def read(sz)
      @inbuf_r.read(sz)
    end

    def write(buf)
      @outbuf << Bytes.force_binary_encoding(buf)
    end

    def flush
      open unless open?

      @service_exchange.publish(
        @outbuf,
        routing_key: @routing_key,
        correlation_id: generate_uuid,
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
