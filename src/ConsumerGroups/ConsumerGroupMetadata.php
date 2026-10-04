<?php

declare(strict_types=1);

namespace KafkaBus\Metadata\ConsumerGroups;

use KafkaBus\Core\Connections\Config\Options;
use KafkaBus\Core\Connections\Kafka\KafkaConsumerFactory;
use KafkaBus\Core\Consumers\ConsumerConfig;
use KafkaBus\Metadata\Exceptions\TopicNotFoundException;
use KafkaBus\Metadata\Topics\PartitionMetadata;
use KafkaBus\Metadata\Topics\TopicsMetadata;
use RdKafka\Exception;
use RdKafka\KafkaConsumer;
use RdKafka\TopicPartition;

final class ConsumerGroupMetadata implements ConsumerGroupMetadataInterface
{
    private ?KafkaConsumer $consumer = null;

    private readonly KafkaConsumerFactory $consumerFactory;

    public function __construct(
        Options $options,
        private readonly ConsumerConfig $consumerConfig,
        private readonly TopicsMetadata $topics,
    ) {
        $this->consumerFactory = new KafkaConsumerFactory($options);
    }

    /**
     * @return list<ConsumerPartition>
     *
     * @throws Exception
     * @throws TopicNotFoundException
     */
    public function partitions(string $topicName): array
    {
        $topicPartitions = array_map(
            static fn (PartitionMetadata $partition) => new TopicPartition($topicName, $partition->id),
            $this->topics->get($topicName)->partitions,
        );

        /** @var list<TopicPartition> $committed */
        $committed = $this->consumer()
            ->getCommittedOffsets($topicPartitions, 10_000);

        return array_map(
            fn (TopicPartition $topicPartition) => $this->makeConsumerPartition($topicName, $topicPartition),
            $committed,
        );
    }

    /**
     * @param list<PartitionOffset> $offsets
     *
     * @throws Exception
     */
    public function commit(array $offsets): void
    {
        $topicPartitions = array_map(
            static fn (PartitionOffset $offset) => new TopicPartition($offset->topicName, $offset->partition, $offset->offset),
            $offsets,
        );

        $this->consumer()
            ->commit($topicPartitions);
    }

    private function makeConsumerPartition(string $topicName, TopicPartition $topicPartition): ConsumerPartition
    {
        $partition = $topicPartition->getPartition();

        [$minOffset, $maxOffset] = $this->watermarkOffsets($topicName, $partition);

        return new ConsumerPartition(
            id: $partition,
            topicName: $topicName,
            currentOffset: $topicPartition->getOffset(),
            minOffset: $minOffset,
            maxOffset: $maxOffset,
        );
    }

    /**
     * @return array{0: int, 1: int}
     */
    private function watermarkOffsets(string $topicName, int $partition): array
    {
        $low = 0;
        $high = 0;

        $this->consumer()
            ->queryWatermarkOffsets($topicName, $partition, $low, $high, 1000);

        return [$low, $high];
    }

    private function consumer(): KafkaConsumer
    {
        return $this->consumer ??= $this->consumerFactory
            ->make($this->consumerConfig);
    }
}
