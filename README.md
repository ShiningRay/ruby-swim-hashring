# Ruby SWIM Microservices Framework

A robust implementation of the SWIM (Scalable Weakly-consistent Infection-style Process Group Membership) protocol in Ruby, designed for building reliable distributed systems and microservices.

## Features

- **Distributed Node Discovery**: Automatic node discovery and membership management
- **Failure Detection**: Fast and accurate failure detection with configurable timeouts
- **State Synchronization**: Efficient distributed state management with version control
- **Metadata Management**: Flexible metadata storage and synchronization
- **HTTP Monitoring**: Built-in HTTP endpoints for monitoring and debugging
- **Configurable Logging**: Multiple log levels for different debugging needs

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'ruby-swim-microservices'
```

Or install it directly:

```bash
gem install ruby-swim-microservices
```

## Quick Start

1. Create a new node:

```ruby
require 'swim'

# Initialize a node
node = Swim::Protocol.new('localhost', 3000)

# Add some metadata
node.set_metadata("#{node.host}:#{node.port}:role", 'primary')
node.set_metadata("#{node.host}:#{node.port}:region", 'us-west')

# Start the node
node.start
```

2. Run the example cluster:

```bash
# Start a cluster with default settings
ruby examples/cluster.rb

# Start with custom settings
ruby examples/cluster.rb --nodes 5 --base-port 3000 --log-level DEBUG
```

## Configuration

The framework supports various configuration options:

### Node Configuration

- `host`: Host address (default: localhost)
- `port`: Port number for SWIM protocol
- `seeds`: List of seed nodes for joining the cluster
- `initial_metadata`: Initial metadata for the node

### Protocol Settings

- `PROTOCOL_PERIOD`: Main protocol loop interval (1.0 seconds)
- `PING_TIMEOUT`: Direct ping timeout (0.5 seconds)
- `PING_REQUEST_TIMEOUT`: Indirect ping timeout (0.5 seconds)
- `SYNC_INTERVAL`: State synchronization interval (10.0 seconds)

### Timeouts

- `PING_TIMEOUT`: 5 seconds
- `SUSPICIOUS_TIMEOUT`: 10 seconds
- `FAILED_TIMEOUT`: 30 seconds

## HTTP Endpoints

The framework provides several HTTP endpoints for monitoring:

- `GET /status`: Basic node status and statistics
- `GET /members`: List of all cluster members and their status
- `GET /metadata`: Node metadata information
- `GET /state`: Current distributed state

## Architecture

### Components

1. **Protocol**
   - Node discovery and membership management
   - Failure detection
   - Message handling
   - State synchronization

2. **StateManager**
   - Distributed state management
   - Version control
   - State merging
   - Change notifications

3. **Member**
   - Member status tracking
   - Timeout management
   - Health monitoring

4. **HTTPServer**
   - Monitoring endpoints
   - Status reporting
   - Debug information

### Message Types

- `join`: Node joining request
- `members`: Member list exchange
- `ping`: Health check
- `ack`: Ping acknowledgment
- `ping_req`: Indirect ping request
- `update`: Status update
- `state_sync`: Full state synchronization
- `state_update`: Incremental state update

## Development

### Running Tests

```bash
rake test
```

### Debugging

Use different log levels for debugging:

```bash
ruby examples/cluster.rb --log-level DEBUG
ruby examples/cluster.rb --log-level INFO
ruby examples/cluster.rb --log-level WARN
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -am 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Based on the [SWIM paper](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf) by Abhinandan Das, Indranil Gupta, and Ashish Motivala
- Inspired by various SWIM implementations in the open-source community

## Status

This project is actively maintained and used in production environments. Please report any issues or feature requests through the issue tracker.
