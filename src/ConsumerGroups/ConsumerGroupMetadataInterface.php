<?php

declare(strict_types=1);

namespace KafkaBus\Metadata\ConsumerGroups;

use KafkaBus\Metadata\Exceptions\TopicNotFoundException;

interface ConsumerGroupMetadataInterface
{
    /**
     * @return list<ConsumerPartition>
     *
     * @throws TopicNotFoundException
     */
    public function partitions(string $topicName): array;

    /**
     * @param list<PartitionOffset> $offsets
     */
    public function commit(array $offsets): void;
}
