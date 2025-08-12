# frozen_string_literal: true

require 'json'
require_relative 'rabbit_mq_consumer/rabbit_mq_connection'

class RabbitMQConsumer
  attr_reader :routing_key, :exchange_name, :main_q_name, :delay_q_name, :dlx_name, :dead_q_name, :retry_delay, :max_retries, :connection, :handler

  def initialize(exchange_name, routing_key, retry_delay: 10_000, max_retries: 5, &block)
    @routing_key = routing_key
    @exchange_name = exchange_name              # Основной topic exchange для публикации сообщений от приложений
    @main_q_name = "#{exchange_name}_main_q"    # Основная очередь для обработки сообщений
    @delay_q_name = "#{exchange_name}_delay_q"  # Очередь с задержкой (delay queue) для повторных попыток обработки
    @dlx_name = "#{exchange_name}_dlx"          # Dead Letter Exchange — направляет сообщения в delay или dead очереди
    @dead_q_name = "#{exchange_name}_dead_q"    # Очередь "мертвых сообщений" для сообщений, у которых исчерпаны попытки
    @retry_delay = retry_delay                  # Время ожидания перед повторной попыткой обработки сообщения после неудачи(в миллисекундах)
    @max_retries = max_retries                  # Максимальное количество попыток повторной обработки сообщения
    @connection = RabbitMQConnection.connection
    @handler = block # сохраняем переданный блок
  end

  def call
    connection.start
    channel = connection.create_channel
    channel.prefetch(1)

    main_exchange = channel.topic(exchange_name, durable: true)
    dlx_exchange = channel.direct(dlx_name, durable: true)

    main_queue = channel.queue(
      main_q_name,
      durable: true,
      arguments: { 'x-dead-letter-exchange' => dlx_name, 'x-dead-letter-routing-key' => delay_q_name }
    )

    delay_queue = channel.queue(
      delay_q_name,
      durable: true,
      arguments: { 'x-message-ttl' => retry_delay, 'x-dead-letter-exchange' => exchange_name, 'x-dead-letter-routing-key' => routing_key }
    )

    dead_queue = channel.queue(dead_q_name, durable: true)

    main_queue.bind(main_exchange, routing_key: routing_key)
    delay_queue.bind(dlx_exchange, routing_key: delay_q_name)
    dead_queue.bind(dlx_exchange, routing_key: dead_q_name)

    puts " [*] Support chat listening on '#{routing_key}'"

    begin
      subscribe(channel, main_queue)
      loop { sleep 1 }
    rescue Interrupt
      connection.close
    end
  end

  private

  def subscribe(channel, queue)
    queue.subscribe(manual_ack: true, block: false) do |delivery_info, properties, payload|
      data = JSON.parse(payload)
      begin
        # Симуляция ошибки для теста
        raise 'Simulated error' if data['chat_id'] == 'chat123'

        handler&.call(data, delivery_info, properties)
        channel.ack(delivery_info.delivery_tag)
      rescue StandardError => e
        retry_count = properties.headers&.dig('x-death', 0, 'count').to_i
        puts "[!] Failed to process message: #{e.message}"
        puts " [!] Retry attempt: #{retry_count} / #{max_retries}"
        next channel.nack(delivery_info.delivery_tag, false, false) if retry_count < max_retries # Отправляем в delay через DLX — RabbitMQ сам увеличит x-death

        channel.default_exchange.publish(payload, routing_key: dead_q_name, persistent: true) # Перемещаем в dead очередь
        channel.ack(delivery_info.delivery_tag)
        Rollbar.error(e)
      end
    end
  end
end