require 'spec_helper'

RSpec.describe Swim::HashRing do
  let(:nodes) { ['node1:3000', 'node2:3000', 'node3:3000'] }
  let(:ring) { described_class.new(nodes) }

  describe '#initialize' do
    it 'creates a hash ring with given nodes' do
      expect(ring.instance_variable_get(:@nodes)).to match_array(nodes)
    end

    it 'creates a hash ring with default replicas' do
      expect(ring.instance_variable_get(:@replicas)).to eq(Swim::HashRing::DEFAULT_REPLICAS)
    end

    it 'creates a hash ring with custom replicas' do
      custom_ring = described_class.new(nodes, 10)
      expect(custom_ring.instance_variable_get(:@replicas)).to eq(10)
    end
  end

  describe '#add_node' do
    let(:new_node) { 'node4:3000' }

    it 'adds a new node to the ring' do
      ring.add_node(new_node)
      expect(ring.instance_variable_get(:@nodes)).to include(new_node)
    end

    it 'creates virtual nodes for the new node' do
      ring.add_node(new_node)
      virtual_nodes = ring.instance_variable_get(:@ring).values.count(new_node)
      expect(virtual_nodes).to eq(Swim::HashRing::DEFAULT_REPLICAS)
    end
  end

  describe '#remove_node' do
    let(:node_to_remove) { 'node1:3000' }

    it 'removes the node from the ring' do
      ring.remove_node(node_to_remove)
      expect(ring.instance_variable_get(:@nodes)).not_to include(node_to_remove)
    end

    it 'removes all virtual nodes of the removed node' do
      ring.remove_node(node_to_remove)
      remaining_nodes = ring.instance_variable_get(:@ring).values
      expect(remaining_nodes).not_to include(node_to_remove)
    end
  end

  describe '#get_node' do
    it 'returns nil for empty ring' do
      empty_ring = described_class.new
      expect(empty_ring.get_node('key1')).to be_nil
    end

    it 'returns consistent node for the same key' do
      key = 'test_key'
      first_node = ring.get_node(key)
      10.times do
        expect(ring.get_node(key)).to eq(first_node)
      end
    end

    it 'distributes keys across nodes' do
      keys = (1..1000).map { |i| "key#{i}" }
      node_counts = Hash.new(0)
      keys.each do |key|
        node = ring.get_node(key)
        node_counts[node] += 1
      end

      # Check if all nodes are used
      expect(node_counts.keys).to match_array(nodes)

      # Check if distribution is relatively even (within 20% of mean)
      mean = keys.length.to_f / nodes.length
      node_counts.each_value do |count|
        expect(count).to be_within(mean * 0.2).of(mean)
      end
    end
  end
end
