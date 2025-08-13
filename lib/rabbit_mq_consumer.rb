# frozen_string_literal: true

require 'json'
require_relative 'rabbit_mq_consumer/rabbit_mq_connection'

class RabbitMQConsumer
  attr_reader :routing_key, :exchange_name, :queue_name, :old_queue_name, :connection, :handler

  def initialize(exchange_name, routing_key, version: 1, &block)
    @routing_key = routing_key
    @exchange_name = exchange_name
    # Добавляем версионность к именам очередей
    @queue_name = "v#{version}_#{exchange_name}_#{routing_key}"
    @old_queue_name = "v#{version - 1}_#{exchange_name}_#{routing_key}"

    @connection = RabbitMQConnection.connection
    @handler = block
  end

  def call
    connection.start
    channel = connection.create_channel
    channel.prefetch(1)

    exchange = channel.topic(exchange_name, durable: true)
    queue = channel.queue( queue_name, durable: true)
    queue.bind(exchange, routing_key: routing_key)

    old_queue = channel.queue(old_queue_name, durable: true)
    old_queue.delete

    puts " [*] Listening on routing key: '#{routing_key}'"

    queue.subscribe(manual_ack: true, block: true) do |delivery_info, properties, payload|
      process_message(channel, delivery_info, properties, payload)
    end
  rescue Interrupt
    connection.close
  end

  private

  def process_message(channel, delivery_info, properties, payload)
    data = JSON.parse(payload)
    handler&.call(data, delivery_info, properties)
    channel.ack(delivery_info.delivery_tag)
  end
end