<?php

declare(strict_types=1);

namespace KafkaBus\Metadata\Partitions;

use KafkaBus\Core\Topics\Topic;

final readonly class CommitOffsetResult
{
    public function __construct(
        public Topic $topic,
        public int   $partition,
        public int   $oldOffset,
        public int   $newOffset,
    ) {
    }
}
