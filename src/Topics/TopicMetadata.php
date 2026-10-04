<?php

declare(strict_types=1);

namespace KafkaBus\Metadata\Topics;

final readonly class TopicMetadata
{
    /**
     * @param list<PartitionMetadata> $partitions
     */
    public function __construct(
        public string $name,
        public array $partitions = [],
    ) {
    }
}
