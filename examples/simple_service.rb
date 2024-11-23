require_relative '../lib/swim/service'

# Create a simple service
service = Swim::Service.new('example_service', 'localhost', 3001)

# Register some handlers
service.register_handler('/hello') do |payload|
  { message: "Hello, #{payload['name']}!" }
end

service.register_handler('/echo') do |payload|
  { echo: payload }
end

# Start the service
puts "Starting service on localhost:3000..."
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
