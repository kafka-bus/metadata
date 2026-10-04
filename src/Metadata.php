<?php

declare(strict_types=1);

namespace KafkaBus\Metadata;

use KafkaBus\Core\Connections\Config\Options;
use KafkaBus\Core\Connections\ConnectionInterface;
use KafkaBus\Core\Consumers\ConsumerConfig;
use KafkaBus\Core\Topics\Topic;
use KafkaBus\Metadata\ConsumerGroups\ConsumerGroupMetadata;
use KafkaBus\Metadata\Partitions\Partitions;
use KafkaBus\Metadata\Topics\TopicsMetadata;

final class Metadata
{
    private ?TopicsMetadata $topics = null;

    public function __construct(
        private readonly Options $options,
    ) {
    }

    public static function fromConnection(ConnectionInterface $connection): self
    {
        return new self($connection->getOptions());
    }

    public function topics(): TopicsMetadata
    {
        return $this->topics ??= new TopicsMetadata($this->options);
    }

    public function consumerGroup(ConsumerConfig $config): ConsumerGroupMetadata
    {
        return new ConsumerGroupMetadata($this->options, $config, $this->topics());
    }

    /**
     * @param list<Topic> $topics
     */
    public function partitions(array $topics, ConsumerConfig $config): Partitions
    {
        return new Partitions($topics, $this->consumerGroup($config));
    }
}
