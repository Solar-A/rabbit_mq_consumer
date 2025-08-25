# frozen_string_literal: true

require 'json'
require_relative 'rabbit_mq_consumer/rabbit_mq_connection'

class RabbitMQConsumer
  attr_reader :routing_key, :exchange_name, :queue_prefix, :connection, :handler, :shard_count, :version

  def initialize(exchange_name, routing_key, version: 1, shards: 1, &block)
    @exchange_name = exchange_name
    @routing_key   = routing_key
    @version       = version
    @shard_count   = shards

    @connection = RabbitMQConnection.connection
    @handler = block
    @queue_prefix  = "#{exchange_name}_#{routing_key}.shard:"
  end

  def call
    connection.start
    channel = connection.create_channel
    channel.prefetch(1)

    exchange = channel.topic(exchange_name, durable: true)

    shard_count.times do |shard_id|
      queue_name  = "v#{version}_#{queue_prefix}#{shard_id}"
      queue = channel.queue(queue_name, durable: true)
      queue.bind(exchange, routing_key: "#{routing_key}.shard:#{shard_id}")

      destroy_old_queue!(channel, shard_id)
      puts " [*] Listening on #{queue_name}"

      queue.subscribe(manual_ack: true, block: false) do |delivery_info, properties, payload|
        process_message(channel, delivery_info, properties, payload)
      end
    end

    sleep # держим процесс
  rescue Interrupt
    connection.close
  end

  private

  def destroy_old_queue!(channel, shard_id)
    old_queue_name = "v#{version - 1}_#{queue_prefix}#{shard_id}"
    old_queue = channel.queue(old_queue_name, durable: true)
    old_queue.delete
  end
  
  def process_message(channel, delivery_info, properties, payload)
    channel.ack(delivery_info.delivery_tag)
    data = JSON.parse(payload)
    handler&.call(data, delivery_info, properties)
  end
end
