# Ruby SWIM Microservices Framework

A distributed microservices framework implemented in Ruby using the SWIM protocol and Consistent Hashing.

## Features

- SWIM Protocol Implementation
  - Failure Detection
  - Membership Management
  - Gossip-based Communication
- Consistent Hash Ring for Service Distribution
- Simple Service Registration and Discovery
- Request Routing Based on Service Names

## Installation

1. Clone the repository
2. Install dependencies:

```bash
bundle install
```

## Usage

### Creating a Simple Service

```ruby
require 'swim/service'

# Create a service instance
service = Swim::Service.new('my_service', 'localhost', 3000)

# Register request handlers
service.register_handler('/hello') do |payload|
  { message: "Hello, #{payload['name']}!" }
end

# Start the service
service.start
```

### Creating a Cluster

To create a cluster of services, you need to provide seed nodes when initializing services:

```ruby
# First node (seed)
service1 = Swim::Service.new('service1', 'localhost', 3000)

# Additional nodes with seed information
service2 = Swim::Service.new('service2', 'localhost', 3001, ['localhost:3000'])
service3 = Swim::Service.new('service3', 'localhost', 3002, ['localhost:3000'])
```

### Making Requests

Services can route requests to other services in the cluster:

```ruby
# Route a request to a service
result = service.route_request('target_service', '/hello', { name: 'World' })
```

## Architecture

### SWIM Protocol

The SWIM protocol is implemented using three main components:

1. Failure Detection: Regular ping-based health checks
2. Dissemination: Gossip-based information sharing
3. Membership: Dynamic member list management

### Consistent Hashing

The framework uses consistent hashing to distribute services across the cluster, ensuring:

- Even distribution of services
- Minimal redistribution when nodes join/leave
- Predictable service location

## Dependencies

- eventmachine: Event-driven I/O
- concurrent-ruby: Thread-safe data structures
- msgpack: Efficient binary serialization
- connection_pool: Connection pooling
- digest-murmurhash: Consistent hashing implementation

## License

MIT License
