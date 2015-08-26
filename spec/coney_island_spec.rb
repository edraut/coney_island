require 'spec_helper'

describe ConeyIsland do
  it 'has a version number' do
    expect(ConeyIsland::VERSION).not_to be nil
  end

  describe "Public API" do
    describe ".logger" do
      it "returns a Logger instance" do
        expect(subject.logger).to be_a Logger
      end
    end

    describe "Configuration methods" do
      [:connection, :publisher_connection, :subscriber_connection,
      :notifier, :max_network_retries, :max_network_retries,
      :network_retry_seed, :network_retry_interval].each do |method|
        it "responds to #{method}" do
          expect(subject).to respond_to(method)
        end
      end
    end

    describe "Publisher methods" do
      [:run_inline, :running_inline?, :stop_running_inline, :cache_jobs,
      :stop_caching_jobs, :flush_jobs, :submit].each do |method|
        it "responds to #{method}" do
          expect(subject).to respond_to(method)
        end
      end
    end

    describe "Worker methods" do
      it "responds to initialize_background" do
        expect(subject).to respond_to :initialize_background
      end
    end

    describe ".configuration" do
      it "responds with a Configuration object" do
        expect(subject.configuration).to be_a ConeyIsland::Configuration
      end
    end
  end

end
