# frozen_string_literal: true

require './lib/rabbit_mq_consumer'

task real_time_chat_consumer: :environment do
  RabbitMQConsumer.new('amicusmeus', 'real_time_chat.*.*') do |data, delivery_info, _properties|
    app = delivery_info.routing_key.split('.')[1]
  end.call
end