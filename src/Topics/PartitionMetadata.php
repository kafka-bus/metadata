<?php

declare(strict_types=1);

namespace KafkaBus\Metadata\Topics;

final readonly class PartitionMetadata
{
    public function __construct(
        public int $id,
        public int $offset,
    ) {
    }
}
