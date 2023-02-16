require "rdkafka"

module Agents
  class KafkaConsumerAgent < Agent
    include FormConfigurable
#    can_dry_run!
    no_bulk_receive!
    default_schedule 'every_1m'

    description <<-MD
      The Kafka Consumer Agent consumes messages from a Kafka topic and creates events from them.
      
      It requires the `rdkafka` gem.
      
      Set the following options:
      
      The `topic` is the name of the topic to consume.

      The `brokers` is a comma-separated list of Kafka brokers to connect to.

      The `debug` can add verbosity.

      Set `expected_update_period_in_days` to the maximum amount of time that you'd expect to pass between Events being created by this Agent.

    MD

    event_description <<-MD
      Events look like this:

          {
            "message": "Payload 88",
            "topic": "test",
            "partition": 0,
            "offset": 5688,
            "key": "Key 88"
          }
    MD

    def default_options
      {
        'topic' => "test",
        'brokers' => "kafka:9092",
        'debug' => 'false',
        'limit' => '10',
        'expected_receive_period_in_days' => '2'
      }
    end

    form_configurable :topic, type: :string
    form_configurable :brokers, type: :string
    form_configurable :limit, type: :string
    form_configurable :debug, type: :boolean
    form_configurable :expected_receive_period_in_days, type: :string
    def validate_options
      errors.add(:base, "topic is required") unless options["topic"].present?

      errors.add(:base, "brokers is required") unless options["brokers"].present?

      errors.add(:base, "limit is required") unless options["limit"].present?

      if options.has_key?('debug') && boolify(options['debug']).nil?
        errors.add(:base, "if provided, debug must be true or false")
      end
    end

    def working?
      event_created_within?(interpolated["expected_update_period_in_days"]) && !recent_error_logs?
    end

    def check
      config = {
        :"bootstrap.servers" => interpolated['brokers'],
        :"group.id" => "huginn-kafka-consumer"
      }
      consumer = Rdkafka::Config.new(config).consumer
      consumer.subscribe(interpolated['topic'])
      
      i=0
      consumer.each do |message|
        if interpolated['debug'] == 'true'
          log "Message received: #{message}"
        end
        create_event :payload => {
          "message" => message.payload,
          "topic" => message.topic,
          "partition" => message.partition,
          "offset" => message.offset,
          "key" => message.key,
        }
        i+=1
        if i >= interpolated['limit'].to_i
          consumer.close
          break
        end
      end
    end
  end
end
