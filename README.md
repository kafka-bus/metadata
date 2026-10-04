# Kafka Bus Metadata

[![Latest Version on Packagist](https://img.shields.io/packagist/v/kafka-bus/metadata.svg?style=flat-square)](https://packagist.org/packages/kafka-bus/metadata)

Kafka cluster metadata for [Kafka Bus](https://github.com/kafka-bus/kafka-bus): list topics and partitions, inspect consumer group offsets and manually set them — independent of any particular consumer/worker implementation.

## Installation

You can install the package via composer:

```bash
composer require kafka-bus/metadata
```

## Usage

```php
use KafkaBus\Core\Consumers\ConsumerConfig;
use KafkaBus\Metadata\Metadata;
use KafkaBus\Metadata\Partitions\CommitOffset;
use KafkaBus\Metadata\Partitions\Offset;

$metadata = Metadata::fromConnection($connection);

// All topics and partitions known to the broker
foreach ($metadata->topics()->list() as $topic) {
    foreach ($topic->partitions as $partition) {
        echo "$topic->name#$partition->id [$partition->offset]\n";
    }
}

// Consumer group offsets for your own topics
$partitions = $metadata->partitions(
    topics: [$topicRegistry->get('products')],
    config: new ConsumerConfig(additionalOptions: ['group.id' => 'products-microservice']),
    ownerName: 'products-microservice',
);

foreach ($partitions->list() as $partition) {
    echo "{$partition->topic->name}#$partition->id C:$partition->currentOffset MIN:$partition->minOffset MAX:$partition->maxOffset\n";
}

// Rewind partition 0 to the earliest offset (use RD_KAFKA_PARTITION_UA for all partitions)
$partitions->setOffset(new CommitOffset($topicRegistry->get('products'), 0, Offset::Early));
```

## Changelog

Please see [CHANGELOG](CHANGELOG.md) for more information on what has changed recently.

## Security Vulnerabilities

Please review [our security policy](../../security/policy) on how to report security vulnerabilities.

## Credits

- [Kirill Popkov](https://github.com/popkovkirill)
- [All Contributors](../../contributors)

## License

The MIT License (MIT). Please see [License File](LICENSE.md) for more information.
