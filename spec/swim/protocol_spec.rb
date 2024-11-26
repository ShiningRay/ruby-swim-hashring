require 'spec_helper'

RSpec.describe Swim::Protocol do
  let(:host) { '127.0.0.1' }
  let(:port) { 7946 }
  let(:protocol) { described_class.new(host, port) }

  describe '#initialize' do
    it 'creates a new protocol instance' do
      expect(protocol).to be_a(described_class)
      expect(protocol.host).to eq(host)
      expect(protocol.port).to eq(port)
    end

    it 'initializes with empty seeds' do
      expect(protocol.instance_variable_get(:@seeds)).to be_empty
    end

    it 'sets up the current node in directory' do
      current_node = protocol.directory.current_node
      expect(current_node).not_to be_nil
      expect(current_node.address).to eq("#{host}:#{port}")
    end

    context 'with seeds' do
      let(:seeds) { ['127.0.0.1:7947', '127.0.0.1:7948'] }
      let(:protocol_with_seeds) { described_class.new(host, port, seeds) }

      it 'initializes with seed nodes' do
        expect(protocol_with_seeds.instance_variable_get(:@seeds)).to eq(seeds)
      end
    end
  end

  describe '#start' do
    let(:seeds) { ['127.0.0.1:7947', '127.0.0.1:7948'] }
    let(:protocol_with_seeds) { described_class.new(host, port, seeds) }
    let(:network) { protocol_with_seeds.instance_variable_get(:@network) }

    before do
      allow(network).to receive(:start)
      allow(network).to receive(:send_message)
    end

    it 'starts the network' do
      protocol_with_seeds.start
      expect(network).to have_received(:start)
    end

    it 'sends join messages to seed nodes' do
      protocol_with_seeds.start
      seeds.each do |seed|
        host, port = seed.split(':')
        expect(network).to have_received(:send_message).with(
          an_object_having_attributes(
            type: :join,
            sender: "#{protocol_with_seeds.host}:#{protocol_with_seeds.port}"
          ),
          host,
          port.to_i
        )
      end
    end

    it 'does not send join messages when no seeds' do
      protocol.start
      expect(network).not_to have_received(:send_message)
    end

    it 'sets up periodic tasks' do
      protocol.start
      expect(protocol.instance_variable_get(:@ping_task)).to be_a(Concurrent::TimerTask)
      expect(protocol.instance_variable_get(:@check_task)).to be_a(Concurrent::TimerTask)
    end
  end

  describe '#handle_join' do
    let(:joiner_addr) { '127.0.0.1:7947' }
    let(:network) { protocol.instance_variable_get(:@network) }

    before do
      allow(network).to receive(:send_message)
    end

    it 'adds new member to directory' do
      protocol.send(:handle_join, joiner_addr)
      member = protocol.directory.get_member(joiner_addr)
      expect(member).not_to be_nil
      expect(member.status).to eq(:alive)
    end

    it 'sends ack message to joining member' do
      protocol.send(:handle_join, joiner_addr)
      expect(network).to have_received(:send_message).with(
        an_object_having_attributes(
          type: :ack,
          sender: "#{protocol.host}:#{protocol.port}",
          target: joiner_addr
        ),
        '127.0.0.1',
        7947
      )
    end

    it 'ignores join from self' do
      protocol.send(:handle_join, "#{protocol.host}:#{protocol.port}")
      expect(network).not_to have_received(:send_message)
    end

    it 'updates existing member status to alive' do
      member = Swim::Member.new('127.0.0.1', 7947)
      member.status = :suspect
      protocol.directory.add_member(member)

      protocol.send(:handle_join, joiner_addr)
      expect(member.status).to eq(:alive)
    end
  end

  describe '#handle_ack' do
    let(:sender_addr) { '127.0.0.1:7947' }
    let(:member) { Swim::Member.new('127.0.0.1', 7947) }

    before do
      protocol.directory.add_member(member)
      member.status = :suspect
      member.pending_ping = Time.now
    end

    it 'updates member status to alive' do
      protocol.send(:handle_ack, sender_addr)
      expect(member.status).to eq(:alive)
    end

    it 'clears pending ping' do
      protocol.send(:handle_ack, sender_addr)
      expect(member.pending_ping?).to be false
    end

    it 'ignores ack from unknown member' do
      expect {
        protocol.send(:handle_ack, '127.0.0.1:7948')
      }.not_to change { member.status }
    end
  end

  describe 'member status queries' do
    let(:alive_member) { Swim::Member.new('127.0.0.1', 7947) }
    let(:suspect_member) { Swim::Member.new('127.0.0.1', 7948) }
    let(:dead_member) { Swim::Member.new('127.0.0.1', 7949) }

    before do
      protocol.directory.add_member(alive_member)
      protocol.directory.add_member(suspect_member)
      protocol.directory.add_member(dead_member)
      
      protocol.directory.update_member_status(suspect_member, :suspect)
      protocol.directory.update_member_status(dead_member, :dead)
    end

    describe '#alive_members' do
      it 'returns alive members' do
        expect(protocol.alive_members).to contain_exactly(alive_member)
      end
    end

    describe '#suspect_members' do
      it 'returns suspect members' do
        expect(protocol.suspect_members).to contain_exactly(suspect_member)
      end
    end

    describe '#dead_members' do
      it 'returns dead members' do
        expect(protocol.dead_members).to contain_exactly(dead_member)
      end
    end

    describe '#members' do
      it 'returns all members' do
        expect(protocol.members).to contain_exactly(alive_member, suspect_member, dead_member)
      end
    end
  end
end
