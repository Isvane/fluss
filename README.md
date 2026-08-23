# Fluss

A minimal Go project exploring pub/sub message delivery using Apache Kafka.

This project builds on the concepts from my old project [starmie](https://github.com/Isvane/starmie), shifting from an in-memory BEAM process actor model in Gleam to distributed message streaming with Go and Sarama.

## Overview

- **Topic Provisioning:** Programmatically checks and creates the target topic (`Pokemon`) on boot using Kafka's cluster admin.
- **Async Producer:** Pushes messages non-blocking to the cluster, handling success and error channels concurrently.
- **Consumer Group:** Joins the `pokemon-fans` group, reads messages from available partitions, and manages offset commits.

## Quickstart

```bash
docker compose up -d
```
