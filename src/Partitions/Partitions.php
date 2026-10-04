<?php

declare(strict_types=1);

namespace KafkaBus\Metadata\Partitions;

use KafkaBus\Core\Topics\Topic;
use KafkaBus\Metadata\ConsumerGroups\ConsumerGroupMetadataInterface;
use KafkaBus\Metadata\ConsumerGroups\ConsumerPartition;
use KafkaBus\Metadata\ConsumerGroups\PartitionOffset;
use KafkaBus\Metadata\Exceptions\CannotCommitOffsetException;
use KafkaBus\Metadata\Exceptions\TopicNotFoundException;

final readonly class Partitions implements PartitionsInterface
{
    /**
     * @param list<Topic> $topics
     */
    public function __construct(
        private array                          $topics,
        private ConsumerGroupMetadataInterface $consumerGroup,
    ) {
    }

    /**
     * @return iterable<TopicPartition>
     */
    public function list(): iterable
    {
        foreach ($this->topics as $topic) {
            yield from $this->topicPartitions($topic);
        }
    }

    /**
     * @param CommitOffset $commitOffset
     * @return CommitOffsetResult[]
     */
    public function setOffset(CommitOffset $commitOffset): array
    {
        $this->throwIfTopicNotOwned($commitOffset->topic);

        try {
            $partitions = $this->partitionsById($commitOffset->topic);
        }
        catch (TopicNotFoundException $exception) {
            throw new CannotCommitOffsetException(
                "Topic [{$commitOffset->topic->name}] not found.",
                previous: $exception,
            );
        }

        $partitionOffsets = $commitOffset->partition === RD_KAFKA_PARTITION_UA
            ? $this->offsetsForAll($commitOffset, $partitions)
            : $this->offsetsForOne($commitOffset, $partitions);

        $this->consumerGroup
            ->commit($partitionOffsets);

        return array_map(
            static fn (PartitionOffset $partitionOffset) => new CommitOffsetResult(
                topic: $commitOffset->topic,
                partition: $partitionOffset->partition,
                oldOffset: $partitions[$partitionOffset->partition]->currentOffset,
                newOffset: $partitionOffset->offset,
            ),
            $partitionOffsets,
        );
    }

    /**
     * @return iterable<TopicPartition>
     */
    private function topicPartitions(Topic $topic): iterable
    {
        try {
            $partitions = $this->consumerGroup
                ->partitions($topic->name);
        }
        catch (TopicNotFoundException) {
            yield new TopicPartition(id: -1, topic: $topic);

            return;
        }

        foreach ($partitions as $partition) {
            yield new TopicPartition(
                id: $partition->id,
                topic: $topic,
                currentOffset: $partition->currentOffset,
                minOffset: $partition->minOffset,
                maxOffset: $partition->maxOffset,
            );
        }
    }

    /**
     * @throws CannotCommitOffsetException
     */
    private function throwIfTopicNotOwned(Topic $topic): void
    {
        foreach ($this->topics as $ownedTopic) {
            if ($ownedTopic->key === $topic->key) {
                return;
            }
        }

        throw new CannotCommitOffsetException("Topic [{$topic->key}] not found.");
    }

    /**
     * @return array<int, ConsumerPartition>
     *
     * @throws TopicNotFoundException
     */
    private function partitionsById(Topic $topic): array
    {
        $partitions = [];

        foreach ($this->consumerGroup->partitions($topic->name) as $partition) {
            $partitions[$partition->id] = $partition;
        }

        return $partitions;
    }

    /**
     * @param array<int, ConsumerPartition> $partitions
     * @return list<PartitionOffset>
     *
     * @throws CannotCommitOffsetException
     */
    private function offsetsForOne(CommitOffset $commitOffset, array $partitions): array
    {
        $partition = $partitions[$commitOffset->partition]
            ?? throw new CannotCommitOffsetException(
                "Partition [{$commitOffset->partition}] not found for topic [{$commitOffset->topic->name}]."
            );

        return [$this->partitionOffset($commitOffset, $partition)];
    }

    /**
     * @param array<int, ConsumerPartition> $partitions
     * @return list<PartitionOffset>
     *
     * @throws CannotCommitOffsetException
     */
    private function offsetsForAll(CommitOffset $commitOffset, array $partitions): array
    {
        return array_values(array_map(
            fn (ConsumerPartition $partition) => $this->partitionOffset($commitOffset, $partition),
            $partitions,
        ));
    }

    /**
     * @throws CannotCommitOffsetException
     */
    private function partitionOffset(CommitOffset $commitOffset, ConsumerPartition $partition): PartitionOffset
    {
        $offset = match ($commitOffset->offset) {
            Offset::Early => $partition->minOffset,
            Offset::Latest => $partition->maxOffset,
            default => $commitOffset->offset,
        };

        if ($offset < $partition->minOffset || $offset > $partition->maxOffset) {
            throw new CannotCommitOffsetException(
                "Offset [$offset] is out of bounds [{$partition->minOffset}, {$partition->maxOffset}]."
            );
        }

        return new PartitionOffset($commitOffset->topic->name, $partition->id, $offset);
    }
}
