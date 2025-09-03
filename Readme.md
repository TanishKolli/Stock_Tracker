
# Simple end to end data streaming
    Kafka Topic -> Spark Streaming (Window()) = Data Aggregated -> Cassandra -> BackEnd (Websocket) -> Dashboard UI
<p>
	<a  target="_blank">
			<img src="https://github.com/tiangolo/fastapi/workflows/Test/badge.svg?event=push&branch=master" alt="Test">
	</a>
	<a ><img alt="Static Badge" src="https://img.shields.io/badge/docker-supported-brightgreen?logo=docker"></a>
	<a ><img alt="Static Badge" src="https://img.shields.io/badge/kubernetes-processing-red?logo=kubernetes"></a>
	<a><img alt="Static Badge" <img alt="Static Badge" src="https://img.shields.io/badge/golang-1.20-brightgreen?logo=go"></a>
</p>

## Introduction

This repository contains the source code and configuration for a real-time data processing pipeline that aggregates data from a Kafka topic, performs real-time analytics using Spark Streaming, stores the aggregated data in Cassandra, and updates a dashboard UI in real-time through a WebSocket connection.

## Architecture

![Architecture](./docs/architecture.png)

## Components

### 1. Kafka Topic

- Data is ingested into the pipeline through a Kafka topic.
- Kafka is a distributed event streaming platform, providing a scalable and fault-tolerant mechanism for data ingestion.

### 2. Spark Streaming (Window())

- Utilizing Spark Streaming for real-time data processing.
- The `Window()` function is applied for windowed operations to aggregate data over specific time intervals.

### 3. Data Aggregated

- Data is aggregated within the Spark Streaming step using various aggregation functions.
- Common operations include summing, averaging, counting, etc., depending on the specific use case.

### 4. Cassandra

- Aggregated data is stored in Cassandra, a highly scalable NoSQL database.
- Cassandra is chosen for its ability to handle large volumes of data across multiple nodes.

### 5. BackEnd (WebSocket)

- The backend of the application communicates with the front end through a WebSocket connection.
- WebSocket enables bidirectional communication, allowing real-time updates to be sent from the server to the client.

### 6. Dashboard UI

- The Dashboard UI provides a user interface for visualizing and interacting with real-time aggregated data.
- Updates are received in real-time through the WebSocket connection, ensuring the dashboard reflects the latest information.

## Getting Started

To set up and run the real-time data processing pipeline, follow the steps outlined in the [Installation Guide](#) and [Configuration Documentation](#).

## Usage

### Prerequisites

- Docker
- Docker Compose

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/anthoai97/simple-end-to-end-data-streaming
   cd simple-end-to-end-data-streaming
   docker-compose up

## Owner

<table>
  <tr>
    <td align="center"><a href="https://github.com/anthoai97"><img src="https://avatars.githubusercontent.com/u/85854989?v=4" width="100px;" alt="An Quach"/><br /><sub><b>An Thoai</b></sub></a><br /></td>
    </tr>
</table>

## License

This project is licensed under the terms of the MIT license.
