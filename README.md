# go-libdht

[![ProbeLab](https://img.shields.io/badge/made%20by-ProbeLab-blue.svg)](https://probelab.io)
[![GoDoc](https://pkg.go.dev/badge/github.com/plprobelab/go-libdht)](https://pkg.go.dev/github.com/plprobelab/go-libdht)
[![Build status](https://img.shields.io/github/actions/workflow/status/plprobelab/go-libdht/go-test.yml?branch=main)](https://github.com/plprobelab/go-libdht/actions)

`go-libdht` is a generic toolbox designed for the implementation and experimentation of Distributed Hash Tables (DHT) in Go. It establishes foundational types and interfaces applicable across a broad spectrum of DHTs, especially those sharing a similar topology. By offering reusable components like keys and routing tables, `go-libdht` streamlines the DHT implementation process. Using `go-libdht`, developers can seamlessly craft their own DHTs using the provided modular components.

## What defines a Distributed Hash Table?

A Distributed Hash Table (DHT) is often defined by its protocol, which determines the allocation of values to specific nodes in a distributed key-value storage system. This allocation should facilitate efficient lookups.

Keys and node identifiers are typically mapped to a Point within a topology dictated by the DHT's architecture. Utilizing a metric that quantifies the distance between any two points in this structure ensures that keys are allocated to the nearest nodes within the topology.

For peak efficiency and scalability in a DHT, it's ideal for nodes to form a [Small World Network](https://en.wikipedia.org/wiki/Small-world_network). In such a setup, each node maintains information on $O(log(n))$ remote peers, and the complexity of lookups similarly scales as $O(log(n))$. Nodes are tasked with monitoring two categories of peers:

* _Nearby Peers_: These are proximate within the DHT's topology. Their primary function is to ensure routing soundness, guaranteeing that every participating peer remains accessible since they are known by neighboring nodes.
* _Faraway Peers_: Positioned more distantly within the topology, these peers bolster lookup speeds, facilitating rapid routing to distant locations in the topology.

The specifics of how nodes select _nearby_ and _faraway_ peers are determined by the individual design nuances of each DHT topology.

## What is in the scope of `go-libdht`

The following generic components can be used by many DHT implementations.

* **Node identifier interface (`NodeID`)**: As a DHT is a distributed system, nodes need a way to identify each other in order to communicate with one another. The `NodeID` interface defines how a node should be addressed in a DHT implementation.
* **Point interface**: A DHT is a distributed key-value store, where each key is identified as a `Point` in the underlying DHT topology. It is essential to map `NodeID`s to a `Point` since entries are typically assigned to nodes nearest to the content, based on a specific distance metric inherent to the DHT topology.
* **Routing Table implementations**: (topology specific): The routing table specifies which remote nodes should be recorded to ensure routing soundness and quick lookups. Each DHT topology possesses a unique methodology for selecting which peers remain within its routing table. `go-libdht` offers generic routing table implementations for various chosen DHT topologies, ready for integration into DHT implementations. For optimal performance, it's recommended to customize the routing table based on the application's requirements, network architecture, and churn dynamics.

## What is out of scope of `go-libdht`

The components listed below are typically specific to the DHT implementation, and may not be universally applicable.

* **DHT records types**: Each DHT implementation needs to define record types for the data that is being stored in the DHT. The role of `go-libdht` is to help with routing matters, but not with data format. It is however recommended to take inspiration from record types of [existing DHT implementations](#users-of-go-libdht) using `go-libdht`. 
* **Wire formats**: Wire formats are highly dependant on the DHT record types and application needs.
* **DHT server behavior**: The server behavior outlines how a node reacts to requests from remote peers. This process is often application specific and depends on the request and record types, hence it should not be standardized.

## Usage

Simply import the building blocks that you need to your DHT implementation.

## Current DHT topologies

Currently, Kademlia is the sole DHT topology that offers building blocks. We anticipate adding building blocks for other DHT topologies in the future.

### Kademlia

Generic Kademlia interfaces can be found in [`kad.go`](kad/kad.go), and include `Key`, `NodeID` and `RoutingTable`. Additional noteworthy components include various `Key` [implementations](kad/key/), a [Binary Trie](kad/trie/) structure, and a [Routing Table](kad/triert/) that leverages the Binary Trie.

## Users of `go-libdht`

It's enouraged to review other DHT implementations for guidance. Certain components, while not sufficiently generic for inclusion in go-libdht, can be reused from existing DHT solutions.

* [**`zikade`**](https://github.com/plprobelab/zikade): Most recent Go implementation of [libp2p Kademlia DHT specification](https://github.com/libp2p/specs/tree/master/kad-dht)

## Future work

* Import more generic Kademlia building blocks from [`zikade`](https://github.com/plprobelab/zikade)
* Add interfaces and building blocks other DHT topologies (e.g Chord, CAN, etc.)
* Define generic interfaces for all DHTs (e.g Point in topology, Distance function, lookup algorithms etc.)

### Standardizing new components

After a component has proven its utility in a DHT implementation and is recognized as broadly applicable for other DHTs, it becomes eligible for integration into `go-libdht`.

## License

[SPDX-License-Identifier: Apache-2.0 OR MIT](./LICENSE.md)