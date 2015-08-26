require 'spec_helper'

describe ConeyIsland::Configuration do

  subject { described_class.new }

  describe "#connection" do
    it "responds with a hash" do
      expect(subject.connection).to be_a Hash
    end

    it "starts pointing to localhost" do
      expect(subject.connection[:host]).to eq "127.0.0.1"
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
    pending
  end

  describe "#max_network_retries" do
    pending
  end

  describe "#network_retry_seed" do
    pending
  end

  describe "#network_retry_interval" do
    pending
  end

  describe "#delay_seed" do
    pending
  end

  describe "#notifier" do
    pending
  end


end
