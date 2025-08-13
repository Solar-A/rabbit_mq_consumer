# frozen_string_literal: true

require 'json'
require_relative 'rabbit_mq_consumer/rabbit_mq_connection'

class RabbitMQConsumer
  attr_reader :routing_key, :exchange_name, :main_q_name, :delay_q_name, :dlx_name, :dead_q_name, :retry_delay, :max_retries,
              :connection, :handler, :max_threads

  def initialize(exchange_name, routing_key, max_threads: 1, retry_delay: 10_000, max_retries: 5, &block)
    @routing_key = routing_key
    @exchange_name = exchange_name              # Основной topic exchange для публикации сообщений от приложений
    @main_q_name = "#{exchange_name}_main_q"    # Основная очередь для обработки сообщений
    @delay_q_name = "#{exchange_name}_delay_q"  # Очередь с задержкой (delay queue) для повторных попыток обработки
    @dlx_name = "#{exchange_name}_dlx"          # Dead Letter Exchange — направляет сообщения в delay или dead очереди
    @dead_q_name = "#{exchange_name}_dead_q"    # Очередь "мертвых сообщений" для сообщений, у которых исчерпаны попытки
    @retry_delay = retry_delay                  # Время ожидания перед повторной попыткой обработки сообщения после неудачи(в миллисекундах)
    @max_retries = max_retries                  # Максимальное количество попыток повторной обработки сообщения
    @max_threads = max_threads
    @connection = RabbitMQConnection.connection
    @handler = block # сохраняем переданный блок
  end

  def call
    connection.start
    channel = connection.create_channel
    channel.prefetch(max_threads)

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

    puts " [*] Listening on routing key: '#{routing_key}', max threads: #{max_threads}"

    semaphore = SizedQueue.new(max_threads) if max_threads > 1

    main_queue.subscribe(manual_ack: true, block: true) do |delivery_info, properties, payload|
      if max_threads > 1
        semaphore << true
        Thread.new do
          process_message(channel, delivery_info, properties, payload)
          semaphore.pop
        end
      else
        process_message(channel, delivery_info, properties, payload)
      end
    end
  rescue Interrupt
    connection.close
  end

  private

  def process_message(channel, delivery_info, properties, payload)
    begin
      data = JSON.parse(payload)
      handler&.call(data, delivery_info, properties)
      channel.ack(delivery_info.delivery_tag)
    rescue StandardError => e
      handle_error(channel, delivery_info, properties, payload, e)
    end
  end

  def handle_error(channel, delivery_info, properties, payload, error)
    retry_count = properties.headers&.dig('x-death', 0, 'count').to_i
    puts "[!] Failed: #{error.message} (attempt #{retry_count + 1}/#{max_retries})"

    if retry_count >= max_retries
      # Отправляем в dead_queue
      channel.default_exchange.publish(
        payload,
        routing_key: dead_q_name,
        persistent: true
      )
      Rollbar.error(error) if defined?(Rollbar)
      channel.ack(delivery_info.delivery_tag)
    else
      # Отправляем в DLX -> delay -> main автоматически через nack
      channel.nack(delivery_info.delivery_tag, false, false)
    end
  end
end