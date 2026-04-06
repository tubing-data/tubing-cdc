# Project context

When I joined the second company in my career, I was assigned to build a CDC system. At that time, I realized that when the amount of data in the enterprise was gradually increasing, such a system was needed to handle data synchronization related work, but before that, we needed a lightweight solution. This project aims to design a lightweight, out-of-the-box system to help the booming system quickly establish a data synchronization module.

## Long-term goal (DBLog)

The intended end state is an implementation aligned with *DBLog: A Watermark Based Change-Data-Capture Framework* by Andreas Andreakis and Ioannis Papapanagiotou ([arXiv:2010.12597](https://arxiv.org/abs/2010.12597), 2020). A copy of the paper lives at [2010.12597v1.pdf](2010.12597v1.pdf). DBLog interleaves transaction-log events with chunked, primary-key-ordered `SELECT` full-state capture using a **watermark table** on the source, so log processing does not stall for long periods, history order is preserved (no time-travel), and table locks are avoided.

Full citation: [references.md](references.md).

## Current phase

The code today is **phase 1**: a MySQL **binlog-only** pipeline—[go-mysql canal](https://github.com/go-mysql-org/go-mysql), a small `TubingCDC` API, optional **binlog position** persistence (Badger + Redis), and pluggable row handlers and sinks. Watermarked chunk dumps, unified log/snapshot event envelopes, and leader-based HA from the paper are **not implemented yet** (see [coverage-vs-dblog.md](coverage-vs-dblog.md) and [roadmap.md](roadmap.md)).

It wraps go-mysql canal for MySQL binlog consumption and exposes a small `TubingCDC` API plus pluggable event handlers.

## Related docs

| Doc | Purpose |
|-----|---------|
| [coverage-vs-dblog.md](coverage-vs-dblog.md) | Paper vs implementation matrix |
| [architecture.md](architecture.md) | Runtime flow diagram |
| [usage.md](usage.md) | API usage |
| [position-persistence.md](position-persistence.md) | Badger + Redis positions |
| [event-handlers.md](event-handlers.md) | Handlers, sinks, transforms |
| [development.md](development.md) | Local testing |
| [roadmap.md](roadmap.md) | Planned phases |
