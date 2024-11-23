require 'spec_helper'

RSpec.describe Swim::StateManager do
  let(:state_manager) { described_class.new }

  describe '#initialize' do
    it 'creates an empty data store' do
      expect(state_manager.data).to be_empty
    end

    it 'starts with version 0' do
      expect(state_manager.version).to eq(0)
    end
  end

  describe '#set' do
    it 'sets a value and increments version' do
      state_manager.set('key1', 'value1')
      expect(state_manager.get('key1')).to eq('value1')
      expect(state_manager.version).to eq(1)
    end

    it 'does not increment version if value is unchanged' do
      state_manager.set('key1', 'value1')
      initial_version = state_manager.version
      state_manager.set('key1', 'value1')
      expect(state_manager.version).to eq(initial_version)
    end

    it 'notifies subscribers of changes' do
      changes = []
      state_manager.subscribe do |key, value, operation|
        changes << [key, value, operation]
      end

      state_manager.set('key1', 'value1')
      expect(changes).to eq([['key1', 'value1', :set]])
    end
  end

  describe '#delete' do
    before { state_manager.set('key1', 'value1') }

    it 'removes the key and increments version' do
      state_manager.delete('key1')
      expect(state_manager.get('key1')).to be_nil
      expect(state_manager.version).to eq(2)
    end

    it 'notifies subscribers of deletion' do
      changes = []
      state_manager.subscribe do |key, value, operation|
        changes << [key, value, operation]
      end

      state_manager.delete('key1')
      expect(changes).to eq([['key1', nil, :delete]])
    end

    it 'does not increment version if key does not exist' do
      state_manager.delete('non_existent_key')
      expect(state_manager.version).to eq(1)
    end
  end

  describe '#snapshot and #apply_snapshot' do
    before do
      state_manager.set('key1', 'value1')
      state_manager.set('key2', 'value2')
    end

    it 'creates a valid snapshot' do
      snapshot = state_manager.snapshot
      expect(snapshot[:version]).to eq(2)
      expect(snapshot[:data]).to eq({'key1' => 'value1', 'key2' => 'value2'})
      expect(snapshot[:checksum]).to be_a(String)
    end

    it 'applies a valid snapshot' do
      snapshot = state_manager.snapshot
      new_state = described_class.new
      expect(new_state.apply_snapshot(snapshot)).to be true
      expect(new_state.get('key1')).to eq('value1')
      expect(new_state.get('key2')).to eq('value2')
      expect(new_state.version).to eq(2)
    end

    it 'rejects invalid snapshot' do
      snapshot = state_manager.snapshot
      snapshot[:checksum] = 'invalid_checksum'
      new_state = described_class.new
      expect(new_state.apply_snapshot(snapshot)).to be false
    end
  end

  describe '#merge_update' do
    it 'applies multiple updates atomically' do
      updates = [
        ['key1', 'value1', :set],
        ['key2', 'value2', :set],
        ['key1', nil, :delete]
      ]

      state_manager.merge_update(updates)
      expect(state_manager.get('key1')).to be_nil
      expect(state_manager.get('key2')).to eq('value2')
      expect(state_manager.version).to eq(1)
    end
  end
end
