require_relative '../lib/swim/service'

# Enable debug logging
Swim::Logger.debug!

def create_service(name, port, seeds = [])
  service = Swim::Service.new(
    name,
    'localhost',
    port,
    seeds
  )

  # Register some test handlers
  service.register_handler('/ping') do |payload|
    { message: "#{name} received ping", timestamp: Time.now.to_i }
  end

  service.register_handler('/status') do |payload|
    { 
      service: name,
      port: port,
      time: Time.now.to_i
    }
  end

  service
end

# Parse command line arguments
port = ARGV[0]&.to_i || 3000
name = ARGV[1] || "service_#{port}"
seeds = ARGV[2]&.split(',') || []

puts "Starting #{name} on port #{port}"
puts "Seeds: #{seeds.join(', ')}" unless seeds.empty?

# Create and start the service
service = create_service(name, port, seeds)
service.start

# Keep the main thread running
puts "Service is running. Press Ctrl+C to stop."
begin
  sleep
rescue Interrupt
  puts "\nShutting down service..."
  service.stop
  puts "Service stopped."
end
