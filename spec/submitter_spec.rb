require 'spec_helper'

describe ConeyIsland::Submitter do
  subject { described_class }

  described_class::CONEY_METHODS.each do |method|
    it "delegates #{method} to ConeyIsland" do
      expect(ConeyIsland).to receive(method)
      subject.send method
    end
  end

  it "delegates store to RequestStore" do
    expect(RequestStore).to receive(:store)
    subject.send :store
  end

  describe ".run_inline" do
    it "sets running_inline? to true" do
      subject.run_inline
      expect(subject.running_inline?).to eq true
    end
  end

  describe ".stop_running_inline" do
    it "sets running_inline? to false" do
      subject.stop_running_inline
      expect(subject.running_inline?).to eq false
    end
  end

  describe ".running_inline?" do
    it "returns false by default" do
      expect(subject.running_inline?).to eq false
    end
  end

  describe ".submit" do
    let(:args) { [Array, :new] }

    context "when caching jobs" do
      before { subject.cache_jobs }
      it "pushes the job to the cache" do
        expect(subject).to receive(:publish_to_cache).with args
        subject.submit *args
      end
    end

    it "submits! the job" do
      expect(subject).to receive(:submit!).with *args
      subject.submit *args
    end
  end

  describe ".submit!" do
    let(:args) { [Array, :new] }
    let(:expected_hash) { { klass: "Array", method_name: :new }  }

    it "publishes to the queue" do
      expect(subject).to receive(:publish_to_queue).with(anything, anything, hash_including(expected_hash), anything)
      subject.submit! *args
    end

    context "when it's not connected" do
      before do
        allow(subject).to receive(:connected?) { false }
      end
      it "calls connect!" do
        expect(subject).to receive(:connect!)
        subject.submit! *args
      end
    end

    context "with an invalid klass" do
      let(:args) { ['some string', :some_method] }
      it "raises ArgumentError" do
        expect { subject.submit! *args }.to raise_error(ArgumentError, /#{args[0]}/)
      end
    end

    context "with an invalid method_name" do
      let(:args) { [Array, 1] }
      it "raises ArgumentError" do
        expect { subject.submit! *args }.to raise_error(ArgumentError, /#{args[1]}/)
      end
    end

    context "when running inline" do
      before { subject.run_inline    }
      after  { subject.submit! *args }
      it "calls handle_job_inlie with the right arguments" do
        expect(subject).to receive(:handle_job_inline).with(nil, hash_including(expected_hash))
      end
      it "doesn't call any of the queueing stuff" do
        expect(subject).not_to receive(:connect!)
        expect(subject).not_to receive(:publish_to_queue)
        expect(subject).not_to receive(:publish_to_delay_queue)
      end
    end

  end

  describe ".handle_connection" do
    pending
  end

  describe "protected methods" do


    describe ".jobs_cache" do

    end

    describe ".publish_to_cache" do

    end

    describe ".publish_to_queue" do

    end

    describe ".publish_to_delay_queue" do

    end

  end

end
