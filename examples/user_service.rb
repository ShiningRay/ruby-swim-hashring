require_relative '../lib/swim/service'
require 'json'

class UserService
  def initialize(port, seeds = [])
    @users = {}  # Simple in-memory storage
    @service = Swim::Service.new('user_service', 'localhost', port, seeds)
    register_handlers
  end

  def start
    puts "Starting User Service on port #{@service.port}..."
    @service.start
  end

  private

  def register_handlers
    # Create user
    @service.register_handler('/users/create') do |payload|
      begin
        validate_user_payload(payload)
        user_id = generate_user_id
        user = {
          id: user_id,
          name: payload['name'],
          email: payload['email'],
          created_at: Time.now.iso8601
        }
        @users[user_id] = user
        { status: 'success', user: user }
      rescue => e
        { status: 'error', message: e.message }
      end
    end

    # Get user by ID
    @service.register_handler('/users/get') do |payload|
      user_id = payload['id']
      user = @users[user_id]
      if user
        { status: 'success', user: user }
      else
        { status: 'error', message: 'User not found' }
      end
    end

    # Update user
    @service.register_handler('/users/update') do |payload|
      begin
        user_id = payload['id']
        user = @users[user_id]
        return { status: 'error', message: 'User not found' } unless user

        ['name', 'email'].each do |field|
          user[field] = payload[field] if payload[field]
        end
        user['updated_at'] = Time.now.iso8601
        
        { status: 'success', user: user }
      rescue => e
        { status: 'error', message: e.message }
      end
    end

    # Delete user
    @service.register_handler('/users/delete') do |payload|
      user_id = payload['id']
      user = @users.delete(user_id)
      if user
        { status: 'success', message: 'User deleted' }
      else
        { status: 'error', message: 'User not found' }
      end
    end

    # List users
    @service.register_handler('/users/list') do |payload|
      {
        status: 'success',
        users: @users.values,
        total: @users.size
      }
    end

    # Health check
    @service.register_handler('/health') do |payload|
      {
        status: 'success',
        service: 'user_service',
        time: Time.now.iso8601,
        users_count: @users.size
      }
    end
  end

  def validate_user_payload(payload)
    raise 'Name is required' unless payload['name']
    raise 'Email is required' unless payload['email']
    raise 'Invalid email format' unless payload['email'] =~ /\A[\w+\-.]+@[a-z\d\-]+(\.[a-z\d\-]+)*\.[a-z]+\z/i
  end

  def generate_user_id
    "user_#{Time.now.to_i}_#{rand(1000)}"
  end
end

# Parse command line arguments
port = ARGV[0]&.to_i || 3000
seeds = ARGV[1]&.split(',') || []

# Create and start the user service
service = UserService.new(port, seeds)
service.start
