require 'spec_helper'

RSpec.describe Swim::Codec do
  let(:sender) { 'localhost:3000' }
  let(:target) { 'localhost:3001' }
  let(:message) { Swim::Message.ping(sender, target) }

  describe '#encode' do
    it 'raises NotImplementedError' do
      expect {
        described_class.new.encode(message)
      }.to raise_error(NotImplementedError)
    end
  end

  describe '#decode' do
    it 'raises NotImplementedError' do
      expect {
        described_class.new.decode("data")
      }.to raise_error(NotImplementedError)
    end
  end
end

RSpec.describe Swim::MessagePackCodec do
  let(:sender) { 'localhost:3000' }
  let(:target) { 'localhost:3001' }
  let(:data) { { key: 'value' } }
  let(:codec) { described_class.new }

  describe '#encode' do
    context 'with valid message' do
      let(:message) { Swim::Message.ping(sender, target) }

      it 'encodes message to MessagePack format' do
        encoded = codec.encode(message)
        expect(encoded).to be_a(String)
        decoded = MessagePack.unpack(encoded)
        expect(decoded['type']).to eq('ping')
        expect(decoded['sender']).to eq(sender)
        expect(decoded['target']).to eq(target)
      end
    end

    context 'with invalid input' do
      it 'returns nil for nil input' do
        expect(codec.encode(nil)).to be_nil
      end

      it 'returns nil for non-Message input' do
        expect(codec.encode("invalid")).to be_nil
      end
    end
  end

  describe '#decode' do
    context 'with valid data' do
      let(:message) { Swim::Message.ping_req(sender, target, data) }
      let(:encoded) { codec.encode(message) }

      it 'decodes MessagePack data to Message' do
        decoded = codec.decode(encoded)
        expect(decoded).to be_a(Swim::Message)
        expect(decoded.type).to eq(:ping_req)
        expect(decoded.sender).to eq(sender)
        expect(decoded.target).to eq(target)
        expect(decoded.data).to eq(data)
      end
    end

    context 'with invalid data' do
      it 'returns nil for nil input' do
        expect(codec.decode(nil)).to be_nil
      end

      it 'returns nil for empty input' do
        expect(codec.decode('')).to be_nil
      end

      it 'returns nil for invalid MessagePack data' do
        expect(codec.decode('invalid')).to be_nil
      end

      it 'returns nil for non-hash MessagePack data' do
        expect(codec.decode([1,2,3].to_msgpack)).to be_nil
      end
    end
  end
end

RSpec.describe Swim::JsonCodec do
  let(:sender) { 'localhost:3000' }
  let(:target) { 'localhost:3001' }
  let(:data) { { key: 'value' } }
  let(:codec) { described_class.new }

  describe '#encode' do
    context 'with valid message' do
      let(:message) { Swim::Message.ping(sender, target) }

      it 'encodes message to JSON format' do
        encoded = codec.encode(message)
        expect(encoded).to be_a(String)
        decoded = JSON.parse(encoded)
        expect(decoded['type']).to eq('ping')
        expect(decoded['sender']).to eq(sender)
        expect(decoded['target']).to eq(target)
      end
    end

    context 'with invalid input' do
      it 'returns nil for nil input' do
        expect(codec.encode(nil)).to be_nil
      end

      it 'returns nil for non-Message input' do
        expect(codec.encode("invalid")).to be_nil
      end
    end
  end

  describe '#decode' do
    context 'with valid data' do
      let(:message) { Swim::Message.ping_req(sender, target, data) }
      let(:encoded) { codec.encode(message) }

      it 'decodes JSON data to Message' do
        decoded = codec.decode(encoded)
        expect(decoded).to be_a(Swim::Message)
        expect(decoded.type).to eq(:ping_req)
        expect(decoded.sender).to eq(sender)
        expect(decoded.target).to eq(target)
        expect(decoded.data).to eq(data)
      end
    end

    context 'with invalid data' do
      it 'returns nil for nil input' do
        expect(codec.decode(nil)).to be_nil
      end

      it 'returns nil for empty input' do
        expect(codec.decode('')).to be_nil
      end

      it 'returns nil for invalid JSON data' do
        expect(codec.decode('invalid')).to be_nil
      end

      it 'returns nil for non-hash JSON data' do
        expect(codec.decode([1,2,3].to_json)).to be_nil
      end
    end
  end
end
