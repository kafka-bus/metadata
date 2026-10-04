<?php

declare(strict_types=1);

namespace KafkaBus\Metadata\ConsumerGroups;

final readonly class PartitionOffset
{
    public function __construct(
        public string $topicName,
        public int $partition,
        public int $offset,
    ) {
    }
}
