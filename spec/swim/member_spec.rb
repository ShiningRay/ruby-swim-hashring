require 'spec_helper'

RSpec.describe Swim::Member do
  let(:host) { 'localhost' }
  let(:port) { 3000 }
  let(:member) { described_class.new(host, port) }

  describe '#initialization' do
    it 'initializes with default values' do
      expect(member.host).to eq(host)
      expect(member.port).to eq(port)
      expect(member.status).to eq(:alive)
      expect(member.incarnation).to eq(0)
    end
  end

  describe '#status management' do
    it 'allows valid status transitions' do
      expect { member.status = :suspect }.not_to raise_error
      expect(member.status).to eq(:suspect)
      expect(member.suspicious?).to be true

      expect { member.status = :dead }.not_to raise_error
      expect(member.status).to eq(:dead)
      expect(member.failed?).to be true
    end

    it 'rejects invalid status values' do
      expect { member.status = :invalid }.to raise_error(ArgumentError)
      expect { member.status = 'invalid' }.to raise_error(ArgumentError)
    end

    it 'accepts string status values' do
      expect { member.status = 'suspect' }.not_to raise_error
      expect(member.status).to eq(:suspect)
    end

    it 'tracks status change time' do
      time = Time.now
      Timecop.freeze(time) do
        member.status = :suspect
        expect(member.instance_variable_get(:@last_state_change_at)).to eq(time.to_f)
      end
    end
  end

  describe '#check_timeouts' do
    before do
      allow(member).to receive(:mark_suspicious)
      allow(member).to receive(:mark_failed)
    end

    it 'handles suspicious timeouts' do
      member.status = :suspect
      member.instance_variable_set(:@last_state_change_at, Time.now.to_f - described_class::SUSPICIOUS_TIMEOUT - 1)

      expect(member.check_timeouts).to be true
      expect(member).to have_received(:mark_failed)
    end

    it 'handles failed timeouts' do
      member.status = :dead
      member.instance_variable_set(:@last_state_change_at, Time.now.to_f - described_class::FAILED_TIMEOUT - 1)

      expect(member.check_timeouts).to be false
    end
  end

  describe '#serialization' do
    it 'serializes to hash with string keys' do
      time = Time.now
      Timecop.freeze(time) do
        member.instance_variable_set(:@incarnation, 1)
        member.instance_variable_set(:@last_response, time)
        member.instance_variable_set(:@last_state_change_at, time.to_f)

        hash = member.to_h
        expect(hash).to include(
          host: host,
          port: port,
          status: :alive,
          incarnation: 1,
          last_response: time.iso8601,
          last_state_change_at: time.to_f
        )
      end
    end

    it 'serializes to msgpack' do
      expect(member.to_msgpack).to be_a(String)
      decoded = MessagePack.unpack(member.to_msgpack)
      expect(decoded).to include('host' => host)
    end
  end
end
