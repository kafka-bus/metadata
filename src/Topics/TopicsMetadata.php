<?php

declare(strict_types=1);

namespace KafkaBus\Metadata\Topics;

use KafkaBus\Core\Connections\Config\Options;
use KafkaBus\Core\Connections\Kafka\KafkaProducerFactory;
use KafkaBus\Core\Producers\ProducerConfig;
use KafkaBus\Core\Topics\Topic;
use KafkaBus\Metadata\Exceptions\TopicNotFoundException;
use RdKafka\Exception;
use RdKafka\Metadata\Partition;
use RdKafka\Producer as KafkaProducer;
use RdKafka\TopicPartition;

final class TopicsMetadata
{
    private ?KafkaProducer $producer = null;

    private readonly KafkaProducerFactory $producerFactory;

    public function __construct(Options $options)
    {
        $this->producerFactory = new KafkaProducerFactory($options);
    }

    /**
     * @param list<Topic>|null $topics only these topics; null — every topic in the cluster
     * @return list<TopicMetadata>
     *
     * @throws Exception
     */
    public function list(?array $topics = null): array
    {
        return $this->fetch($topics === null ? null : array_map(static fn (Topic $topic) => $topic->name, $topics));
    }

    /**
     * @throws Exception
     * @throws TopicNotFoundException
     */
    public function get(string $topicName): TopicMetadata
    {
        return $this->fetch([$topicName])[0]
            ?? throw new TopicNotFoundException("Topic [$topicName] not found.");
    }

    /**
     * @param list<string>|null $topicNames
     * @return list<TopicMetadata>
     *
     * @throws Exception
     */
    private function fetch(?array $topicNames): array
    {
        $topicsMeta = $this->producer()
            ->getMetadata(true, null, 10_000);

        $topics = [];

        foreach ($topicsMeta->getTopics() as $topicMeta) {
            if ($topicNames !== null && ! \in_array($topicMeta->getTopic(), $topicNames, true)) {
                continue;
            }

            $topicPartitions = array_map(
                static fn (Partition $partition) => new TopicPartition($topicMeta->getTopic(), $partition->getId()),
                iterator_to_array($topicMeta->getPartitions()),
            );

            $partitions = array_map(
                static fn (TopicPartition $topicPartition) => new PartitionMetadata(
                    $topicPartition->getPartition(),
                    $topicPartition->getOffset(),
                ),
                $this->producer()->offsetsForTimes($topicPartitions, 10_000),
            );

            $topics[] = new TopicMetadata($topicMeta->getTopic(), array_values($partitions));
        }

        return $topics;
    }

    private function producer(): KafkaProducer
    {
        return $this->producer ??= $this->producerFactory
            ->make(new ProducerConfig());
    }
}
