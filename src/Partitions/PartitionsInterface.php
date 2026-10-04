<?php

declare(strict_types=1);

namespace KafkaBus\Metadata\Partitions;

use KafkaBus\Metadata\Exceptions\CannotCommitOffsetException;

interface PartitionsInterface
{
    /**
     * @return iterable<TopicPartition>
     */
    public function list(): iterable;

    /**
     * @return list<CommitOffsetResult>
     *
     * @throws CannotCommitOffsetException
     */
    public function setOffset(CommitOffset $commitOffset): array;
}
