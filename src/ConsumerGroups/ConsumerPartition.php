<?php

declare(strict_types=1);

namespace KafkaBus\Metadata\ConsumerGroups;

final readonly class ConsumerPartition
{
    public function __construct(
        public int $id,
        public string $topicName,
        public int $currentOffset = 0,
        public int $minOffset = 0,
        public int $maxOffset = 0,
    ) {
    }
}
