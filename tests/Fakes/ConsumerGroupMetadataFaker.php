<?php

declare(strict_types=1);

namespace KafkaBus\Metadata\Tests\Fakes;

use KafkaBus\Metadata\ConsumerGroups\ConsumerGroupMetadataInterface;
use KafkaBus\Metadata\ConsumerGroups\ConsumerPartition;
use KafkaBus\Metadata\ConsumerGroups\PartitionOffset;
use KafkaBus\Metadata\Exceptions\TopicNotFoundException;

final class ConsumerGroupMetadataFaker implements ConsumerGroupMetadataInterface
{
    /**
     * @var list<PartitionOffset>
     */
    public array $committed = [];

    /**
     * @param array<string, list<ConsumerPartition>> $partitions keyed by topic name
     */
    public function __construct(
        private readonly array $partitions = [],
    ) {
    }

    public function partitions(string $topicName): array
    {
        return $this->partitions[$topicName]
            ?? throw new TopicNotFoundException("Topic [$topicName] not found.");
    }

    public function commit(array $offsets): void
    {
        array_push($this->committed, ...$offsets);
    }
}
