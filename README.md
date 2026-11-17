# Kafka Bus Metadata

Inspect Apache Kafka topics, partitions, watermarks and committed consumer group offsets from PHP. Update offsets for selected topics and partitions with bounds validation.

## Installation

Requires PHP 8.2+ and `ext-rdkafka`. Composer installs Kafka Bus Core.

```bash
composer require kafka-bus/metadata
```

## Quick start: inspect a topic

With Kafka at `localhost:9092` and an existing `orders.events` topic, save this next to `vendor/` and run it with PHP:

```php
<?php

require __DIR__ . '/vendor/autoload.php';

use KafkaBus\Core\Connections\Config\KafkaConnectionConfig;
use KafkaBus\Core\Connections\Registry\ConnectionRegistry;
use KafkaBus\Metadata\Metadata;

$connections = ConnectionRegistry::default();
$metadata = Metadata::fromConnection($connections->connection('default'));
$topic = $metadata->topics()->get('orders.events');

foreach ($topic->partitions as $partition) {
    echo $topic->name . '#' . $partition->id . PHP_EOL;
}
```

`$metadata->topics()->list()` lists cluster topics. Pass a list of Core `Topic` objects to filter it.

## Inspect group offsets

Continue from the example above:

```php
use KafkaBus\Core\Consumers\ConsumerConfig;
use KafkaBus\Core\Topics\Topic;

$orders = new Topic('orders.events', 'orders');
$config = new ConsumerConfig(additionalOptions: ['group.id' => 'orders-service']);
$partitions = $metadata->partitions([$orders], $config);

foreach ($partitions->list() as $partition) {
    printf(
        "%s#%d: committed=%d, low=%d, high=%d\n",
        $partition->topic->name,
        $partition->id,
        $partition->currentOffset,
        $partition->minOffset,
        $partition->maxOffset,
    );
}
```

The high watermark is the next offset after the last available record. A group without a committed offset may return a Kafka invalid-offset sentinel. A missing topic is represented in `Partitions::list()` by a placeholder with partition ID `-1`.

## Change committed offsets

Perform offset administration while the affected consumers are stopped, so they do not overwrite the new offsets. This changes where that group resumes reading:

```php
use KafkaBus\Metadata\Partitions\CommitOffset;
use KafkaBus\Metadata\Partitions\Offset;

$results = $partitions->setOffset(new CommitOffset(
    topic: $orders,
    partition: RD_KAFKA_PARTITION_UA,
    offset: Offset::Early,
));
```

`RD_KAFKA_PARTITION_UA` selects every partition. Use a concrete partition ID for one partition. `Offset::Early` selects its low watermark, `Offset::Latest` its high watermark; an integer selects an explicit offset. Requests outside the available bounds raise `CannotCommitOffsetException`.

Each result contains the topic, partition, old offset and new offset. For lower-level access, `$metadata->consumerGroup($config)` exposes `partitions($topicName)` and `commit($partitionOffsets)`.

[Issues and contributions](https://github.com/kafka-bus/kafka-bus) · [Releases](https://github.com/kafka-bus/kafka-bus/releases) · [MIT License](https://github.com/kafka-bus/kafka-bus/blob/HEAD/LICENSE.md).
