require 'spec_helper'

describe ConeyIsland do
  it 'has a version number' do
    expect(ConeyIsland::VERSION).not_to be nil
  end

  describe "Public API" do
    [:connection, :publisher_connection, :subscriber_connection,
    :notifier, :max_network_retries, :max_network_retries,
    :network_retry_seed, :network_retry_interval].each do |method|
      it "delegates #{method} to configuration" do
        expect(subject.config).to receive(method)
        subject.send method
      end
    end

    [:run_inline, :running_inline?, :stop_running_inline, :cache_jobs,
    :stop_caching_jobs, :flush_jobs, :submit].each do |method|
      it "delegates #{method} to Submitter" do
        expect(ConeyIsland::Submitter).to receive(method)
        subject.send method
      end
    end

    it "delegates initialize_background to Worker" do
      expect(ConeyIsland::Worker).to receive(:initialize_background)
      subject.initialize_background
    end

    describe ".configuration" do
      it "responds with a Configuration object" do
        expect(subject.configuration).to be_a ConeyIsland::Configuration
      end
    end

    describe ".logger" do
      it "returns a Logger instance" do
        expect(subject.logger).to be_a Logger
      end
    end

    describe ".configure" do
      pending
    end

    describe ".poke_the_badger" do
      pending
    end
  end

end
