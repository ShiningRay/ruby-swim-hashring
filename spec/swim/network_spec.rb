require 'spec_helper'
require 'timeout'

module Swim
  RSpec.describe Network do
    let(:host) { '127.0.0.1' }
    let(:port) { 7946 }
    let(:network) { Network.new(host, port) }
    let(:message) { Message.ping('sender:1234', 'target:5678') }

    describe '#initialize' do
      it 'creates a network instance' do
        expect(network).to be_a(Network)
        expect(network.host).to eq(host)
        expect(network.port).to eq(port)
        expect(network).not_to be_running
      end
    end

    describe '#start' do
      after { network.stop }

      it 'starts the network' do
        network.start
        expect(network).to be_running
        expect(network.socket).to be_a(UDPSocket)
      end

      it 'is idempotent' do
        network.start
        socket = network.socket
        network.start
        expect(network.socket).to eq(socket)
      end
    end

    describe '#stop' do
      before { network.start }

      it 'stops the network' do
        network.stop
        expect(network).not_to be_running
        expect(network.socket).to be_nil
      end

      it 'is idempotent' do
        network.stop
        expect { network.stop }.not_to raise_error
      end
    end

    describe '#send_message' do
      let(:target_host) { '127.0.0.1' }
      let(:target_port) { 7947 }
      let!(:receiver) { UDPSocket.new.tap { |s| s.bind(target_host, target_port) } }

      before do
        network.start
      end

      after do
        network.stop
        receiver.close
      end

      it 'sends a message successfully' do
        success = network.send_message(message, target_host, target_port)
        expect(success).to be true

        Timeout.timeout(10) do
          data, _ = receiver.recvfrom(65535)
          decoded_message = MessagePackCodec.new.decode(data)
          expect(decoded_message.type).to eq(message.type)
          expect(decoded_message.sender).to eq(message.sender)
          expect(decoded_message.target).to eq(message.target)          
        end

      end

      it 'returns false when network is not running' do
        network.stop
        success = network.send_message(message, target_host, target_port)
        expect(success).to be false
      end

      it 'returns false for invalid message' do
        success = network.send_message(nil, target_host, target_port)
        expect(success).to be false
      end
    end

    describe '#broadcast_message' do
      let(:targets) { ['127.0.0.1:7947', '127.0.0.1:7948'] }
      let!(:receivers) do
        targets.map do |addr|
          host, port = addr.split(':')
          UDPSocket.new.tap { |s| s.bind(host, port.to_i) }
        end
      end

      before do
        network.start
      end

      after do
        network.stop
        receivers.each(&:close)
      end

      it 'broadcasts message to multiple targets' do
        count = network.broadcast_message(message, targets)
        expect(count).to eq(targets.size)
        Timeout.timeout(10) do
          receivers.each do |receiver|
            data, _ = receiver.recvfrom(65535)
            decoded_message = MessagePackCodec.new.decode(data)
            expect(decoded_message.type).to eq(message.type)
          end
        end
      end

      it 'returns 0 when network is not running' do
        network.stop
        count = network.broadcast_message(message, targets)
        expect(count).to eq(0)
      end

      it 'returns 0 for empty targets' do
        count = network.broadcast_message(message, [])
        expect(count).to eq(0)
      end
    end

    describe 'event broadcasting' do
      let(:listener) { double('listener') }
      let(:target_host) { '127.0.0.1' }
      let(:target_port) { 7947 }

      before do
        network.subscribe(listener)
        network.start
      end

      after { network.stop }

      it 'broadcasts message_sent event' do
        expect(listener).to receive(:message_sent)
          .with(message, target_host, target_port, kind_of(Integer))
        
        network.send_message(message, target_host, target_port)
      end

      it 'broadcasts send_error event' do
        expect(listener).to receive(:send_error)
          .with(kind_of(Exception), message, target_host, target_port)
        
        # Force an error by closing the socket
        network.socket.close
        
        network.send_message(message, target_host, target_port)
      end

      it 'broadcasts message_received event' do
        sender = UDPSocket.new
        expect(listener).to receive(:message_received)
          .with(kind_of(Message), kind_of(String))

        encoded_message = MessagePackCodec.new.encode(message)
        sender.send(encoded_message, 0, host, port)
        
        sleep(1) # Give some time for the message to be processed
        sender.close
      end
    end
  end
end
