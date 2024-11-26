require 'spec_helper'

module Swim
  RSpec.describe Directory do
    let(:directory) { Directory.new }
    let(:member) { Member.new('127.0.0.1', 7946) }
    let(:member2) { Member.new('127.0.0.1', 7947) }

    describe '#initialize' do
      it 'creates an empty directory' do
        expect(directory.size).to eq(0)
      end
    end

    describe '#add_member' do
      it 'adds a member and broadcasts member_joined event' do
        events = []
        directory.on(:member_joined) { |m| events << m }
        directory.add_member(member)
        expect(events).to contain_exactly(member)
      end

      it 'returns false for duplicate member' do
        directory.add_member(member)
        expect(directory.add_member(member)).to be false
      end
    end

    describe '#remove_member' do
      before { directory.add_member(member) }

      it 'removes a member and broadcasts member_left event' do
        events = []
        directory.on(:member_left) { |m| events << m }
        directory.remove_member(member.address)
        expect(events).to contain_exactly(member)
      end

      it 'returns nil for non-existent member' do
        expect(directory.remove_member('invalid:1234')).to be_nil
      end
    end

    describe '#update_member_status' do
      before { directory.add_member(member) }

      it 'broadcasts member_suspected event when marking suspicious' do
        events = []
        directory.on(:member_suspected) { |m| events << m }
        directory.update_member_status(member, :suspect)
        expect(events).to contain_exactly(member)
      end

      it 'broadcasts member_failed event when marking failed' do
        events = []
        directory.on(:member_failed) { |m| events << m }
        directory.update_member_status(member, :dead)
        expect(events).to contain_exactly(member)
      end

      it 'broadcasts member_recovered event when marking alive from suspect' do
        directory.update_member_status(member, :suspect)
        events = []
        directory.on(:member_recovered) { |m| events << m }
        directory.update_member_status(member, :alive)
        expect(events).to contain_exactly(member)
      end
    end

    describe '#members_by_status' do
      before do
        directory.add_member(member)
        directory.add_member(member2)
        directory.update_member_status(member2, :suspect)
      end

      it 'returns members with specified status' do
        expect(directory.members_by_status(:alive)).to contain_exactly(member)
        expect(directory.members_by_status(:suspect)).to contain_exactly(member2)
      end

      it 'returns empty array for status with no members' do
        expect(directory.members_by_status(:dead)).to be_empty
      end
    end

    describe '#clear' do
      before do
        directory.add_member(member)
        directory.add_member(member2)
      end

      it 'removes all members and broadcasts member_left events' do
        events = []
        directory.on(:member_left) { |m| events << m }
        directory.clear
        expect(events).to contain_exactly(member, member2)
        expect(directory.size).to eq(0)
      end
    end
  end
end
