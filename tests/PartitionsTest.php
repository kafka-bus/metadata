<?php

declare(strict_types=1);

namespace KafkaBus\Metadata\Tests;

use KafkaBus\Core\Topics\Topic;
use KafkaBus\Metadata\ConsumerGroups\ConsumerPartition;
use KafkaBus\Metadata\ConsumerGroups\PartitionOffset;
use KafkaBus\Metadata\Exceptions\CannotCommitOffsetException;
use KafkaBus\Metadata\Exceptions\TopicNotFoundException;
use KafkaBus\Metadata\Partitions\CommitOffset;
use KafkaBus\Metadata\Partitions\CommitOffsetResult;
use KafkaBus\Metadata\Partitions\Offset;
use KafkaBus\Metadata\Partitions\Partitions;
use KafkaBus\Metadata\Partitions\TopicPartition;
use KafkaBus\Metadata\Tests\Fakes\ConsumerGroupMetadataFaker;
use Testo\Assert;
use Testo\Expect;
use Testo\Test;

#[Test]
final class PartitionsTest
{
    public function listsPartitionsOfEveryOwnedTopic(): void
    {
        $orders = new Topic('events.orders.1', 'orders');
        $products = new Topic('events.products.1', 'products');

        $consumerGroup = new ConsumerGroupMetadataFaker([
            'events.orders.1' => [
                new ConsumerPartition(0, 'events.orders.1', currentOffset: 5, minOffset: 0, maxOffset: 10),
                new ConsumerPartition(1, 'events.orders.1', currentOffset: 7, minOffset: 2, maxOffset: 9),
            ],
            'events.products.1' => [
                new ConsumerPartition(0, 'events.products.1', currentOffset: 1, minOffset: 0, maxOffset: 3),
            ],
        ]);

        $partitions = new Partitions([$orders, $products], $consumerGroup);

        Assert::equals(iterator_to_array($partitions->list(), false), [
            new TopicPartition(id: 0, topic: $orders, currentOffset: 5, minOffset: 0, maxOffset: 10),
            new TopicPartition(id: 1, topic: $orders, currentOffset: 7, minOffset: 2, maxOffset: 9),
            new TopicPartition(id: 0, topic: $products, currentOffset: 1, minOffset: 0, maxOffset: 3),
        ]);
    }

    public function listsPlaceholderPartitionForTopicMissingInCluster(): void
    {
        $orders = new Topic('events.orders.1', 'orders');

        $partitions = new Partitions([$orders], new ConsumerGroupMetadataFaker());

        Assert::equals(iterator_to_array($partitions->list(), false), [
            new TopicPartition(id: -1, topic: $orders),
        ]);
    }

    public function setsEarlyOffsetForSinglePartition(): void
    {
        [$partitions, $consumerGroup, $orders] = $this->ordersPartitions();

        $results = $partitions->setOffset(new CommitOffset($orders, 1, Offset::Early));

        Assert::equals($consumerGroup->committed, [new PartitionOffset('events.orders.1', 1, 2)]);
        Assert::equals($results, [new CommitOffsetResult($orders, partition: 1, oldOffset: 7, newOffset: 2)]);
    }

    public function setsLatestOffsetForSinglePartition(): void
    {
        [$partitions, $consumerGroup, $orders] = $this->ordersPartitions();

        $results = $partitions->setOffset(new CommitOffset($orders, 0, Offset::Latest));

        Assert::equals($consumerGroup->committed, [new PartitionOffset('events.orders.1', 0, 10)]);
        Assert::equals($results, [new CommitOffsetResult($orders, partition: 0, oldOffset: 5, newOffset: 10)]);
    }

    public function setsExplicitOffsetWithinBounds(): void
    {
        [$partitions, $consumerGroup, $orders] = $this->ordersPartitions();

        $partitions->setOffset(new CommitOffset($orders, 0, 3));

        Assert::equals($consumerGroup->committed, [new PartitionOffset('events.orders.1', 0, 3)]);
    }

    public function setsOffsetForAllPartitionsUsingTheirOwnIds(): void
    {
        [$partitions, $consumerGroup, $orders] = $this->ordersPartitions();

        $results = $partitions->setOffset(new CommitOffset($orders, RD_KAFKA_PARTITION_UA, Offset::Early));

        Assert::equals($consumerGroup->committed, [
            new PartitionOffset('events.orders.1', 0, 0),
            new PartitionOffset('events.orders.1', 1, 2),
        ]);
        Assert::equals($results, [
            new CommitOffsetResult($orders, partition: 0, oldOffset: 5, newOffset: 0),
            new CommitOffsetResult($orders, partition: 1, oldOffset: 7, newOffset: 2),
        ]);
    }

    public function doesNotCommitWhenOffsetIsOutOfBounds(): void
    {
        [$partitions, $consumerGroup, $orders] = $this->ordersPartitions();

        try {
            $partitions->setOffset(new CommitOffset($orders, 0, 11));

            Assert::fail('CannotCommitOffsetException was expected');
        }
        catch (CannotCommitOffsetException $exception) {
            Assert::string($exception->getMessage())->contains('out of bounds [0, 10]');
        }

        Assert::same($consumerGroup->committed, []);
    }

    public function doesNotCommitAnythingWhenOnePartitionOfManyIsOutOfBounds(): void
    {
        [$partitions, $consumerGroup, $orders] = $this->ordersPartitions();

        try {
            // 1 is valid for partition 0 ([0, 10]) but not for partition 1 ([2, 9]).
            $partitions->setOffset(new CommitOffset($orders, RD_KAFKA_PARTITION_UA, 1));

            Assert::fail('CannotCommitOffsetException was expected');
        }
        catch (CannotCommitOffsetException) {
        }

        Assert::same($consumerGroup->committed, []);
    }

    public function throwsWhenPartitionDoesNotExist(): void
    {
        [$partitions, , $orders] = $this->ordersPartitions();

        Expect::exception(CannotCommitOffsetException::class)
            ->withMessageContaining('Partition [5] not found');

        $partitions->setOffset(new CommitOffset($orders, 5, Offset::Early));
    }

    public function throwsWhenTopicIsNotOwned(): void
    {
        [$partitions] = $this->ordersPartitions();

        Expect::exception(CannotCommitOffsetException::class)
            ->withMessageContaining('Topic [products] not found');

        $partitions->setOffset(new CommitOffset(new Topic('events.products.1', 'products'), 0, Offset::Early));
    }

    public function throwsWhenOwnedTopicIsMissingInCluster(): void
    {
        $orders = new Topic('events.orders.1', 'orders');

        $partitions = new Partitions([$orders], new ConsumerGroupMetadataFaker());

        try {
            $partitions->setOffset(new CommitOffset($orders, 0, Offset::Early));

            Assert::fail('CannotCommitOffsetException was expected');
        }
        catch (CannotCommitOffsetException $exception) {
            Assert::string($exception->getMessage())->contains('Topic [events.orders.1] not found');
            Assert::instanceOf($exception->getPrevious(), TopicNotFoundException::class);
        }
    }

    /**
     * @return array{Partitions, ConsumerGroupMetadataFaker, Topic}
     */
    private function ordersPartitions(): array
    {
        $orders = new Topic('events.orders.1', 'orders');

        $consumerGroup = new ConsumerGroupMetadataFaker([
            'events.orders.1' => [
                new ConsumerPartition(0, 'events.orders.1', currentOffset: 5, minOffset: 0, maxOffset: 10),
                new ConsumerPartition(1, 'events.orders.1', currentOffset: 7, minOffset: 2, maxOffset: 9),
            ],
        ]);

        return [new Partitions([$orders], $consumerGroup), $consumerGroup, $orders];
    }
}
