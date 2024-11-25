require 'rspec'
require 'webrick'
require 'json'
require 'timecop'
require 'concurrent'
require 'rspec/wait'

# Add lib to load path
$LOAD_PATH.unshift File.expand_path('../lib', __dir__)
require 'swim'

# Configure RSpec
RSpec.configure do |config|
  # Include wait matchers
  config.include RSpec::Wait
  config.wait_timeout = 10 # seconds
  config.wait_delay = 0.1 # seconds

  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
  end

  config.shared_context_metadata_behavior = :apply_to_host_groups
  config.filter_run_when_matching :focus
  config.example_status_persistence_file_path = "spec/examples.txt"
  config.disable_monkey_patching!
  config.warnings = false
  config.order = :random
  Kernel.srand config.seed

  # Test helpers
  def setup_node(host = 'localhost', port = nil, seeds = [])
    port ||= next_available_port
    Swim::Protocol.new(host, port, seeds)
  end

  def next_available_port
    server = TCPServer.new('127.0.0.1', 0)
    port = server.addr[1]
    server.close
    port
  end

  # Helper for waiting on conditions
  def wait_until(timeout: 5, delay: 0.1)
    start_time = Time.now
    while Time.now - start_time < timeout
      return true if yield
      sleep delay
    end
    false
  end

  # Clean up resources
  config.after(:each) do
    Timecop.return
  end
end
