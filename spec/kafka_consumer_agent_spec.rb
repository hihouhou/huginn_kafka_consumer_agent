require 'rails_helper'
require 'huginn_agent/spec_helper'

describe Agents::KafkaConsumerAgent do
  before(:each) do
    @valid_options = Agents::KafkaConsumerAgent.new.default_options
    @checker = Agents::KafkaConsumerAgent.new(:name => "KafkaConsumerAgent", :options => @valid_options)
    @checker.user = users(:bob)
    @checker.save!
  end

  pending "add specs here"
end
