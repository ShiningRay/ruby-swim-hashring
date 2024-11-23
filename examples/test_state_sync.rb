require_relative '../lib/swim/service'
require 'json'

class StateTestClient
  def initialize(port = 3100, target_service = 'state_service')
    @service = Swim::Service.new('test_client', 'localhost', port, ['localhost:3000'])
    @target_service = target_service
    sleep(2) # Wait for cluster to stabilize
  end

  def set_state(key, value)
    payload = { key: key, value: value }
    response = @service.route_request(@target_service, '/state/set', payload)
    puts "\nSetting state: #{key} = #{value}"
    puts JSON.pretty_generate(response)
    response
  end

  def get_state(key)
    response = @service.route_request(@target_service, '/state/get', { key: key })
    puts "\nGetting state: #{key}"
    puts JSON.pretty_generate(response)
    response
  end

  def delete_state(key)
    response = @service.route_request(@target_service, '/state/delete', { key: key })
    puts "\nDeleting state: #{key}"
    puts JSON.pretty_generate(response)
    response
  end

  def run_test_sequence
    puts "Starting state sync test sequence..."
    
    # Set some initial state
    set_state('counter', 1)
    set_state('message', 'Hello, World!')
    set_state('timestamp', Time.now.to_i)
    
    sleep(2) # Wait for sync
    
    # Read state back
    get_state('counter')
    get_state('message')
    get_state('timestamp')
    
    sleep(2)
    
    # Update state
    set_state('counter', 2)
    set_state('message', 'Updated message')
    
    sleep(2)
    
    # Verify updates
    get_state('counter')
    get_state('message')
    
    sleep(2)
    
    # Delete some state
    delete_state('timestamp')
    
    sleep(2)
    
    # Verify deletion
    get_state('timestamp')
  end
end

# Create and run test client
client = StateTestClient.new
client.run_test_sequence
