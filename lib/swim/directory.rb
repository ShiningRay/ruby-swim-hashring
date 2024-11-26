require 'wisper'
require 'concurrent'
require_relative 'member'
require_relative 'logger'

module Swim
  # Directory manages all member nodes in the SWIM cluster.
  # It provides member lookup functionality and publishes events when members join or leave.
  class Directory
    include Wisper::Publisher

    def initialize
      @members = Concurrent::Map.new
      @mutex = Mutex.new
      Logger.info("Directory initialized")
    end

    # Add a new member to the directory
    # @param member [Member] the member to add
    # @return [Boolean] true if member was added, false if already exists
    def add_member(member)
      return false unless member.is_a?(Member)
      
      @mutex.synchronize do
        member_address = member.address
        return false if @members.key?(member_address)
        
        @members[member_address] = member
        Logger.info("Member added: #{member_address}")
        broadcast(:member_joined, member)
        true
      end
    end

    # Remove a member from the directory
    # @param address [String] the address of the member to remove
    # @return [Member, nil] the removed member or nil if not found
    def remove_member(address)
      @mutex.synchronize do
        member = @members.delete(address)
        if member
          Logger.info("Member removed: #{address}")
          broadcast(:member_left, member)
        end
        member
      end
    end

    # Get a member by address
    # @param address [String] the address of the member
    # @return [Member, nil] the member or nil if not found
    def get_member(address)
      @members[address]
    end

    # Update member status and trigger appropriate events
    # @param member [Member] the member to update
    # @param new_status [Symbol] the new status (:alive, :suspect, :dead)
    def update_member_status(member, new_status)
      return unless member.is_a?(Member)
      
      @mutex.synchronize do
        old_status = member.status
        return if old_status == new_status
        
        case new_status
        when :alive
          member.mark_alive
          broadcast(:member_recovered, member) if old_status != :alive
        when :suspect
          member.mark_suspicious
          broadcast(:member_suspected, member)
        when :dead
          member.mark_failed
          broadcast(:member_failed, member)
          # Optionally remove dead members
          # remove_member(member.address)
        end
      end
    end

    # Get all members
    # @return [Array<Member>] array of all members
    def all_members
      @members.values
    end

    # Get members by status
    # @param status [Symbol] the status to filter by (:alive, :suspect, :dead)
    # @return [Array<Member>] array of members with the specified status
    def members_by_status(status)
      all_members.select { |m| m.status == status }
    end

    # Get alive members
    # @return [Array<Member>] array of alive members
    def alive_members
      members_by_status(:alive)
    end

    # Get suspicious members
    # @return [Array<Member>] array of suspicious members
    def suspicious_members
      members_by_status(:suspect)
    end

    # Get failed members
    # @return [Array<Member>] array of failed members
    def failed_members
      members_by_status(:dead)
    end

    # Check if a member exists
    # @param address [String] the address of the member
    # @return [Boolean] true if member exists
    def member_exists?(address)
      @members.key?(address)
    end

    # Get the number of members
    # @return [Integer] the number of members
    def size
      @members.size
    end

    # Clear all members
    def clear
      @mutex.synchronize do
        old_members = @members.values
        @members.clear
        old_members.each { |m| broadcast(:member_left, m) }
      end
    end
  end
end
