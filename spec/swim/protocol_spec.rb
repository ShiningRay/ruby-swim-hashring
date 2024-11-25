require 'spec_helper'

RSpec.describe Swim::Protocol do
  let(:host) { '127.0.0.1' }
  let(:port) { 7946 }
  let(:protocol) { described_class.new(host, port, [], {}, test_mode: true) }

  describe '#initialize' do
    it 'creates a new protocol instance' do
      expect(protocol).to be_a(described_class)
      expect(protocol.host).to eq(host)
      expect(protocol.port).to eq(port)
    end

    context 'with seeds' do
      let(:seeds) { ['127.0.0.1:7947', '127.0.0.1:7948'] }
      let(:protocol_with_seeds) { described_class.new(host, port, seeds, {}, test_mode: true) }

      it 'initializes with seed nodes' do
        expect(protocol_with_seeds.instance_variable_get(:@seeds)).to eq(seeds)
      end
    end

    context 'with initial metadata' do
      let(:metadata) { { 'key' => 'value' } }
      let(:protocol_with_metadata) { described_class.new(host, port, [], metadata, test_mode: true) }

      it 'initializes with metadata' do
        expect(protocol_with_metadata.metadata['key']).to eq('value')
      end
    end
  end

  describe 'member status queries' do
    let(:alive_member) { Swim::Member.new('127.0.0.1', 7947) }
    let(:suspect_member) { Swim::Member.new('127.0.0.1', 7948) }
    let(:dead_member) { Swim::Member.new('127.0.0.1', 7949) }

    before do
      suspect_member.status = :suspect
      dead_member.status = :dead

      protocol.instance_variable_get(:@members)['127.0.0.1:7947'] = alive_member
      protocol.instance_variable_get(:@members)['127.0.0.1:7948'] = suspect_member
      protocol.instance_variable_get(:@members)['127.0.0.1:7949'] = dead_member
    end

    describe '#alive_members' do
      it 'returns addresses of alive members' do
        expect(protocol.alive_members).to contain_exactly('127.0.0.1:7947')
      end
    end

    describe '#suspect_members' do
      it 'returns addresses of suspect members' do
        expect(protocol.suspect_members).to contain_exactly('127.0.0.1:7948')
      end
    end

    describe '#dead_members' do
      it 'returns addresses of dead members' do
        expect(protocol.dead_members).to contain_exactly('127.0.0.1:7949')
      end
    end
  end

  describe 'metadata management' do
    let(:key) { 'test_key' }
    let(:value) { 'test_value' }

    describe '#get_metadata' do
      before { protocol.set_metadata(key, value) }

      it 'retrieves metadata value' do
        expect(protocol.get_metadata(key)).to eq(value)
      end

      it 'returns nil for non-existent key' do
        expect(protocol.get_metadata('non_existent')).to be_nil
      end
    end

    describe '#set_metadata' do
      it 'sets metadata value' do
        expect(protocol.set_metadata(key, value)).to be true
        expect(protocol.get_metadata(key)).to eq(value)
      end

      it 'returns false for nil key' do
        expect(protocol.set_metadata(nil, value)).to be false
      end
    end

    describe '#delete_metadata' do
      before { protocol.set_metadata(key, value) }

      it 'deletes metadata value' do
        expect(protocol.delete_metadata(key)).to be true
        expect(protocol.get_metadata(key)).to be_nil
      end

      it 'returns false for nil key' do
        expect(protocol.delete_metadata(nil)).to be false
      end
    end
  end

  describe 'lifecycle management' do
    after { protocol.stop }

    describe '#start' do
      it 'starts the protocol' do
        protocol.start
        expect(protocol.instance_variable_get(:@running)).to be true
      end

      it 'is idempotent' do
        protocol.start
        protocol.start
        expect(protocol.instance_variable_get(:@running)).to be true
      end
    end

    describe '#stop' do
      before { protocol.start }

      it 'stops the protocol' do
        protocol.stop
        expect(protocol.instance_variable_get(:@running)).to be false
      end

      it 'is idempotent' do
        protocol.stop
        protocol.stop
        expect(protocol.instance_variable_get(:@running)).to be false
      end
    end
  end

  describe 'event callbacks' do
    describe '#on_member_change' do
      it 'registers member change callback' do
        callback = proc {}
        protocol.on_member_change(&callback)
        callbacks = protocol.instance_variable_get(:@callbacks)
        expect(callbacks).to include(callback)
      end
    end

    describe '#on_metadata_change' do
      it 'registers metadata change callback' do
        callback = proc {}
        protocol.on_metadata_change(&callback)
        callbacks = protocol.instance_variable_get(:@metadata_callbacks)
        expect(callbacks).to include(callback)
      end
    end
  end
end
