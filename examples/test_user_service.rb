require_relative '../lib/swim/service'
require 'json'

# Create a test client service
class TestClient
  def initialize(port = 3100, target_service = 'user_service')
    @service = Swim::Service.new('test_client', 'localhost', port, ['localhost:3000'])
    @target_service = target_service
    sleep(2) # Wait for cluster to stabilize
  end

  def create_user(name, email)
    payload = { name: name, email: email }
    response = @service.route_request(@target_service, '/users/create', payload)
    puts "\nCreating user: #{name}"
    puts JSON.pretty_generate(response)
    response
  end

  def get_user(id)
    response = @service.route_request(@target_service, '/users/get', { id: id })
    puts "\nGetting user: #{id}"
    puts JSON.pretty_generate(response)
    response
  end

  def update_user(id, updates)
    payload = { id: id }.merge(updates)
    response = @service.route_request(@target_service, '/users/update', payload)
    puts "\nUpdating user: #{id}"
    puts JSON.pretty_generate(response)
    response
  end

  def delete_user(id)
    response = @service.route_request(@target_service, '/users/delete', { id: id })
    puts "\nDeleting user: #{id}"
    puts JSON.pretty_generate(response)
    response
  end

  def list_users
    response = @service.route_request(@target_service, '/users/list', {})
    puts "\nListing all users:"
    puts JSON.pretty_generate(response)
    response
  end

  def check_health
    response = @service.route_request(@target_service, '/health', {})
    puts "\nHealth check:"
    puts JSON.pretty_generate(response)
    response
  end

  def run_test_sequence
    puts "Starting test sequence..."
    
    # Health check first
    check_health
    
    # Create users
    user1 = create_user("John Doe", "john@example.com")
    sleep(1) # Add small delay between requests
    
    user2 = create_user("Jane Smith", "jane@example.com")
    sleep(1)
    
    # List all users
    list_users
    sleep(1)
    
    # Get specific user
    if user1['status'] == 'success'
      get_user(user1['user']['id'])
      sleep(1)
    end
    
    # Update user
    if user2['status'] == 'success'
      update_user(user2['user']['id'], { name: "Jane Wilson" })
      sleep(1)
    end
    
    # List users after update
    list_users
    sleep(1)
    
    # Delete user
    if user1['status'] == 'success'
      delete_user(user1['user']['id'])
      sleep(1)
    end
    
    # Final list and health check
    list_users
    sleep(1)
    check_health
  end
end

# Create and run test client
client = TestClient.new
client.run_test_sequence
