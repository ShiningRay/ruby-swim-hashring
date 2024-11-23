require 'spec_helper'

RSpec.describe Swim::Member do
  let(:host) { 'localhost' }
  let(:port) { 3000 }
  let(:incarnation) { 1 }
  let(:member) { described_class.new(host, port, incarnation) }

  describe '#initialize' do
    it 'sets initial values correctly' do
      expect(member.host).to eq(host)
      expect(member.port).to eq(port)
      expect(member.incarnation).to eq(incarnation)
      expect(member.state).to eq(:alive)
      expect(member.last_state_change_at).to be_within(1).of(Time.now.to_f)
    end

    it 'creates correct address string' do
      expect(member.address).to eq("#{host}:#{port}")
    end
  end

  describe '#update' do
    context 'when new incarnation is higher' do
      it 'updates state and incarnation' do
        member.update(:suspect, 2)
        expect(member.state).to eq(:suspect)
        expect(member.incarnation).to eq(2)
      end

      it 'updates last_state_change_at' do
        old_timestamp = member.last_state_change_at
        sleep(0.1)
        member.update(:suspect, 2)
        expect(member.last_state_change_at).to be > old_timestamp
      end
    end

    context 'when new incarnation is lower' do
      it 'does not update state or incarnation' do
        member.update(:suspect, 0)
        expect(member.state).to eq(:alive)
        expect(member.incarnation).to eq(1)
      end
    end

    context 'when new incarnation is equal' do
      it 'updates to more severe state' do
        member.update(:suspect, 1)
        expect(member.state).to eq(:suspect)
        
        member.update(:dead, 1)
        expect(member.state).to eq(:dead)
      end

      it 'does not update to less severe state' do
        member.update(:dead, 1)
        expect(member.state).to eq(:dead)
        
        member.update(:alive, 1)
        expect(member.state).to eq(:dead)
      end
    end
  end

  describe 'state predicates' do
    it 'correctly reports alive state' do
      expect(member.alive?).to be true
      expect(member.suspect?).to be false
      expect(member.dead?).to be false
    end

    it 'correctly reports suspect state' do
      member.update(:suspect, 2)
      expect(member.alive?).to be false
      expect(member.suspect?).to be true
      expect(member.dead?).to be false
    end

    it 'correctly reports dead state' do
      member.update(:dead, 2)
      expect(member.alive?).to be false
      expect(member.suspect?).to be false
      expect(member.dead?).to be true
    end
  end

  describe '#to_msgpack' do
    it 'serializes member data correctly' do
      packed = member.to_msgpack
      unpacked = MessagePack.unpack(packed)
      
      expect(unpacked['host']).to eq(host)
      expect(unpacked['port']).to eq(port)
      expect(unpacked['incarnation']).to eq(incarnation)
      expect(unpacked['state']).to eq('alive')
    end
  end
end
