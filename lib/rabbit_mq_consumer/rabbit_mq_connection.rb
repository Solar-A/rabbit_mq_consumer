# frozen_string_literal: true

require 'bunny'
require 'yaml'
require 'active_support/core_ext/hash/keys'

class RabbitMQConsumer
  class RabbitMQConnection
    class << self
      def connection
        @connection ||= Bunny.new(config)
      end

      private

      def config
        @config ||= ::YAML.load_file('config/rabbitmq.yml').deep_symbolize_keys!
      end
    end
  end
end
