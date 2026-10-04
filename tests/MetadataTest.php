<?php

declare(strict_types=1);

namespace KafkaBus\Metadata\Tests;

use KafkaBus\Core\Consumers\ConsumerConfig;
use KafkaBus\Core\Testing\Connections\ConnectionFaker;
use KafkaBus\Core\Topics\Topic;
use KafkaBus\Core\Topics\TopicRegistry;
use KafkaBus\Metadata\Metadata;
use KafkaBus\Metadata\Partitions\Partitions;
use Testo\Assert;
use Testo\Test;

#[Test]
final class MetadataTest
{
    public function reusesTopicsMetadataInstance(): void
    {
        $metadata = Metadata::fromConnection(new ConnectionFaker(new TopicRegistry()));

        Assert::same($metadata->topics(), $metadata->topics());
    }

    public function buildsPartitionsWithoutConnectingToKafka(): void
    {
        $metadata = Metadata::fromConnection(new ConnectionFaker(new TopicRegistry()));

        $partitions = $metadata->partitions(
            [new Topic('events.orders.1', 'orders')],
            new ConsumerConfig(additionalOptions: ['group.id' => 'test-group']),
        );

        Assert::instanceOf($partitions, Partitions::class);
    }
}
