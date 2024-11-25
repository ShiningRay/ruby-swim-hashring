require 'spec_helper'

RSpec.describe Swim::Message do
  let(:sender) { 'localhost:3000' }
  let(:target) { 'localhost:3001' }
  let(:data) { { key: 'value' } }

  describe '.new' do
    it 'creates a message with valid type' do
      message = described_class.new(:ping, sender, target)
      expect(message.type).to eq(:ping)
      expect(message.sender).to eq(sender)
      expect(message.target).to eq(target)
    end

    it 'raises error for invalid type' do
      expect {
        described_class.new(:invalid_type, sender)
      }.to raise_error(ArgumentError, /Invalid message type/)
    end
  end

  describe 'factory methods' do
    describe '.join' do
      it 'creates a join message' do
        message = described_class.join(sender)
        expect(message.type).to eq(:join)
        expect(message.sender).to eq(sender)
        expect(message.target).to be_nil
      end
    end

    describe '.ping' do
      it 'creates a ping message' do
        message = described_class.ping(sender, target)
        expect(message.type).to eq(:ping)
        expect(message.sender).to eq(sender)
        expect(message.target).to eq(target)
      end
    end

    describe '.ack' do
      it 'creates an ack message' do
        message = described_class.ack(sender, target)
        expect(message.type).to eq(:ack)
        expect(message.sender).to eq(sender)
        expect(message.target).to eq(target)
      end
    end

    describe '.ping_req' do
      it 'creates a ping request message' do
        message = described_class.ping_req(sender, target, data)
        expect(message.type).to eq(:ping_req)
        expect(message.sender).to eq(sender)
        expect(message.target).to eq(target)
        expect(message.data).to eq(data)
      end
    end

    describe '.ping_ack' do
      it 'creates a ping acknowledgment message' do
        message = described_class.ping_ack(sender, target, data)
        expect(message.type).to eq(:ping_ack)
        expect(message.sender).to eq(sender)
        expect(message.target).to eq(target)
        expect(message.data).to eq(data)
      end
    end

    describe '.suspect' do
      it 'creates a suspect message' do
        message = described_class.suspect(sender, target)
        expect(message.type).to eq(:suspect)
        expect(message.sender).to eq(sender)
        expect(message.target).to eq(target)
      end
    end

    describe '.alive' do
      it 'creates an alive message' do
        incarnation = 1
        message = described_class.alive(sender, target, incarnation)
        expect(message.type).to eq(:alive)
        expect(message.sender).to eq(sender)
        expect(message.target).to eq(target)
        expect(message.data).to eq({ incarnation: incarnation })
      end

      it 'creates an alive message without incarnation' do
        message = described_class.alive(sender, target)
        expect(message.type).to eq(:alive)
        expect(message.sender).to eq(sender)
        expect(message.target).to eq(target)
        expect(message.data).to eq({})
      end
    end

    describe '.dead' do
      it 'creates a dead message' do
        message = described_class.dead(sender, target)
        expect(message.type).to eq(:dead)
        expect(message.sender).to eq(sender)
        expect(message.target).to eq(target)
      end
    end

    describe '.metadata' do
      it 'creates a metadata message' do
        metadata = { version: '1.0' }
        message = described_class.metadata(sender, metadata)
        expect(message.type).to eq(:metadata)
        expect(message.sender).to eq(sender)
        expect(message.target).to be_nil
        expect(message.data).to eq(metadata)
      end
    end
  end

  describe '#to_h' do
    it 'converts message to hash' do
      message = described_class.ping(sender, target)
      hash = message.to_h
      expect(hash[:type]).to eq(:ping)
      expect(hash[:sender]).to eq(sender)
      expect(hash[:target]).to eq(target)
      expect(hash[:timestamp]).to be_a(Float)
    end

    it 'excludes nil values' do
      message = described_class.join(sender)
      hash = message.to_h
      expect(hash).not_to have_key(:target)
    end
  end
end
