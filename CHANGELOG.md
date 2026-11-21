# Changelog

All notable changes to this project will be documented in this file.

Note:

- The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
- This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
- How to add an entry is described in [Contributing](README.md#contributing).
- Earlier releases are described in [GitHub Releases](https://github.com/libp2p/go-libp2p-kad-dht/releases).

## Legend

The following emojis are used to highlight certain changes:

- 🛠 - BREAKING CHANGE. Action is required if you use this functionality.
- ✨ - Noteworthy change to be aware of.

## [Unreleased]

### Added

### Changed

### Removed

### Fixed

## [v0.43.1]

### Changed

- upgrade to `go-libp2p` [v0.52.0](https://github.com/libp2p/go-libp2p/releases/tag/v0.52.0) (from v0.50.0), which brings `quic-go` v0.64.1 and releases connections that close before they are accepted. The exported API is unchanged, and Go 1.26 remains the minimum version. [#1311](https://github.com/libp2p/go-libp2p-kad-dht/pull/1311)

### Fixed

- `GetValue` and `SearchValue` with a quorum no longer leave their lookup running once the quorum is reached. Queries still in flight stayed blocked until the caller's context ended, and kept the lookup's goroutines alive with them; with a context that has no deadline, they never exited. [#1309](https://github.com/libp2p/go-libp2p-kad-dht/pull/1309)
- `Provide` with `EnableOptimisticProvide` now returns when the lookup finds no peer to send `ADD_PROVIDER` to, for example because every routing table peer is unreachable. It used to block forever, whatever its context. [#1309](https://github.com/libp2p/go-libp2p-kad-dht/pull/1309)
- `rtrefresh`: a refresh that stops at a common prefix length with no peers now signals completion, so the DHT leaves its bootstrap phase. [#1310](https://github.com/libp2p/go-libp2p-kad-dht/pull/1310)
