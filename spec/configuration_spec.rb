require 'spec_helper'

describe ConeyIsland::Configuration do

  subject { described_class.new }

  describe "#connection" do
    it "responds with a hash" do
      expect(subject.connection).to be_a Hash
    end

    it "defaults to #{described_class::DEFAULT_CONNECTION}" do
      expect(subject.connection).to eq described_class::DEFAULT_CONNECTION
    end
  end

  describe "#publisher_connection" do
    it "responds with the connection by default" do
      expect(subject.publisher_connection).to eq subject.connection
    end

    context "when it's explicitly set" do
      let(:some_value) { {host: "somehost.com"} }
      before { subject.publisher_connection = some_value }

      it "responds with whatever was set" do
        expect(subject.publisher_connection).to eq some_value
      end
    end
  end

  describe "#subscriber_connection" do
    it "responds with the connection by default" do
      expect(subject.subscriber_connection).to eq subject.connection
    end

    context "when it's explicitly set" do
      let(:some_value) { {host: "somehost.com"} }
      before { subject.subscriber_connection = some_value }

      it "responds with whatever was set" do
        expect(subject.subscriber_connection).to eq some_value
      end
    end
  end

  describe "#carousels" do
    it "defaults to #{described_class::DEFAULT_QUEUES}" do
      expect(subject.carousels).to eq described_class::DEFAULT_QUEUES
    end
  end

  describe "#max_network_retries" do
    it "defaults to #{described_class::DEFAULT_NETWORK_RETRIES}" do
      expect(subject.max_network_retries).to eq described_class::DEFAULT_NETWORK_RETRIES
    end
  end

  describe "#network_retry_seed" do
    it "defaults to #{described_class::DEFAULT_NETWORK_RETRY_SEED}" do
      expect(subject.network_retry_seed).to eq described_class::DEFAULT_NETWORK_RETRY_SEED
    end
  end

  describe "#network_retry_interval" do
    it "calculates correctly" do
      expect(subject.network_retry_interval(2)).to eq subject.network_retry_seed ** 2
    end
  end

  describe "#delay_seed" do
    it "defaults to #{described_class::DEFAULT_DELAY_SEED}" do
      expect(subject.delay_seed).to eq described_class::DEFAULT_DELAY_SEED
    end
  end

  describe "#notifier" do
    it "defaults to #{described_class::DEFAULT_NOTIFIER}" do
      expect(subject.notifier).to eq described_class::DEFAULT_NOTIFIER
    end
  end


end
