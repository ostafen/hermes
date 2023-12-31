# Hermes

Hermes implements the **EventStoreDB** [projection subsystem](https://developers.eventstore.com/server/v5/projections.html#introduction-to-projections) on top of **Apachi Kafka**. It is meant to simplify the development of architectures based on **CQRS** and **event-sourcing**.

## Why Hermes?

EventStoreDB **projections** have been originally designed to process large streams of events in a reactive manner. They can solve almost any business problem where near real-time complex event processing is required (for example, monitoring of temperature sensors, reacting to changes in the stock market, etc...).
Kakfa is a de-facto standard when it comes to stream processing, and ships with stateful processing facilities, fault tolerance and processing guarantees. Its partitioned layout makes it suitable for processing thousands of events in a reliable manner.

## Build (Go 1.9+)
Run the following command

```bash
foo@bar$ make build
```

## Local service setup

```yaml
logging:
  level: INFO
  format: JSON

server:
  port: 9175 # http service port

kafka:
  brokers: 
    - "localhost:9092"
```

To start the service, run the command:

```bash
foo@bar$ ./bin/hermes config.yml
```

## Docker-compose setup

```yaml
---
version: '3'
services:
  zookeper:
    ...
  
  kafka:
    ...

  hermes:
    image: ghcr.io/ostafen/hermes:0.0.1-alpha
    ports:
      - '9175:9175'
    environment:
      SERVER_PORT: 9175
      KAFKA_BROKERS: localhost:9092
```

You can find a ready to use docker compose file [here](docker-compose.yml).

## Projections

Projections are defined in Javascript, through a powerful and flexible state transformation API. 
For example, the following snippet of code defines a projection which computes the total number of events having a specific **eventType**.

```js
fromStream('my-stream').
    partitionBy(e => e.eventType).
    when({
        $init: function() {
            return { count: 0 }
        },
        $any: function(state, e) {
            state.count += 1
        }
    }).
    filterBy(s => s.count >= 10).
    transformBy(function(state) {
        return { Total: state.count }
    }).
    outputTo('out-stream')
```

Assuming the projection is stored in a file named `projection.js`, you can use the following command to bootstrap the projection:

```bash
curl -X POST localhost:9175/projections/my-projection -H 'Content-Type: text/javascript' --data-binary @projection.js
```

## Supported Projections Operators

### Selectors

| Selector                    | Description                               |
| --------------------------  | ----------------------------------------- |
| `fromStream({streamId})`    | Selects events from the streamId stream.  |
| `fromStreams()`             | Selects events from the streams supplied.	|


### Filters and Transformations

| Selector                       | Description                                                                                                                                           | Provides                                     |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------- |
| `when(handlers)`               | Allows only the given events of a particular to pass through the projection.                                                                          | transformBy, filterBy, outputTo, outputState |
| `outputState()`                | If the projection is statefull, setting this option produces a stream called `projections-{projection-name}-result` with the state as the event body. | transformBy, filterBy, outputTo              |
| `partitionBy(function(event))` | Partitions a projection by the partition returned from the handler.                                                                                   | transformBy, filterBy, outputTo              |
| `transformBy(function(state))` | Provides the ability to transform the state of a projection by the provided handler.                                                                  | transformBy, filterBy, outputTo, outputState |
| `filterBy(function(state))`    | Causes projection results to be `null` for any state that returns a `false` value from the given predicate.                                           | transformBy, filterBy, outputTo, outputState |


# REST API

- **POST** /projections/{name} - Create a new projections
- **DELETE** /projections/{name} - Delete an existing projections

## Contact
Stefano Scafiti @ostafen

## License
Hermes source code is available under the **MIT** License.

## Contributing

Hermes is actively developed. If you want to contribute to the project, please read the following [contribution guidelines](CODE_OF_CONDUCT.md).