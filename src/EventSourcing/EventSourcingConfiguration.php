<?php


namespace Ecotone\EventSourcing;


use Enqueue\Dbal\DbalConnectionFactory;

class EventSourcingConfiguration
{
    private bool $initializeEventStoreOnStart = LazyProophEventStore::INITIALIZE_ON_STARTUP;
    private int $loadBatchSize = LazyProophEventStore::LOAD_BATCH_SIZE;
    private bool $enableWriteLockStrategy = LazyProophEventStore::DEFAULT_ENABLE_WRITE_LOCK_STRATEGY;
    private string $eventStreamTableName = LazyProophEventStore::DEFAULT_STREAM_TABLE;
    private string $projectionsTable = LazyProophEventStore::DEFAULT_PROJECTIONS_TABLE;
    private string $eventStoreReferenceName;
    private string $projectManagerReferenceName;
    private string $connectionReferenceName;
    private string $persistenceStrategy = LazyProophEventStore::SINGLE_STREAM_PERSISTENCE;

    private function __construct(string $connectionReferenceName = DbalConnectionFactory::class, string $eventStoreReferenceName = EventStore::class, string $projectManagerReferenceName = ProjectionManager::class)
    {
        $this->eventStoreReferenceName = $eventStoreReferenceName;
        $this->projectManagerReferenceName = $projectManagerReferenceName;
        $this->connectionReferenceName = $connectionReferenceName;
    }

    public static function create(string $connectionReferenceName = DbalConnectionFactory::class, string $eventStoreReferenceName = EventStore::class, string $projectManagerReferenceName = ProjectionManager::class) : static
    {
        return new self($connectionReferenceName, $eventStoreReferenceName, $projectManagerReferenceName);
    }

    public static function createWithDefaults() : static
    {
        return new self();
    }

    public function withSingleStreamPersistenceStrategy(): static
    {
        $this->persistenceStrategy = LazyProophEventStore::SINGLE_STREAM_PERSISTENCE;

        return $this;
    }

    public function withStreamPerAggregatePersistenceStrategy(): static
    {
        $this->persistenceStrategy = LazyProophEventStore::AGGREGATE_STREAM_PERSISTENCE;

        return $this;
    }

    public function withInitializeEventStoreOnStart(bool $isInitializedOnStartup) : static
    {
        $this->initializeEventStoreOnStart = $isInitializedOnStartup;

        return $this;
    }

    public function withLoadBatchSize(int $loadBatchSize) : static
    {
        $this->loadBatchSize = $loadBatchSize;

        return $this;
    }

    public function withWriteLockStrategy(bool $enableWriteLockStrategy) : static
    {
        $this->enableWriteLockStrategy = $enableWriteLockStrategy;

        return $this;
    }

    public function withEventStreamTableName(string $eventStreamTableName) : static
    {
        $this->eventStreamTableName = $eventStreamTableName;

        return $this;
    }

    public function withProjectionsTableName(string $projectionsTableName) : static
    {
        $this->projectionsTable = $projectionsTableName;

        return $this;
    }

    public function isInitializedOnStart(): bool
    {
        return $this->initializeEventStoreOnStart;
    }

    public function isUsingSingleStreamStrategy() : bool
    {
        return $this->getPersistenceStrategy() === LazyProophEventStore::SINGLE_STREAM_PERSISTENCE;
    }

    public function isUsingAggregateStreamStrategy() : bool
    {
        return $this->getPersistenceStrategy() === LazyProophEventStore::AGGREGATE_STREAM_PERSISTENCE;
    }

    public function getPersistenceStrategy(): string
    {
        return $this->persistenceStrategy;
    }

    public function getLoadBatchSize(): int
    {
        return $this->loadBatchSize;
    }

    public function isWriteLockStrategyEnabled(): bool
    {
        return $this->enableWriteLockStrategy;
    }

    public function getEventStreamTableName(): string
    {
        return $this->eventStreamTableName;
    }

    public function getProjectionsTable(): string
    {
        return $this->projectionsTable;
    }

    public function getEventStoreReferenceName(): string
    {
        return $this->eventStoreReferenceName;
    }

    public function getProjectManagerReferenceName(): string
    {
        return $this->projectManagerReferenceName;
    }

    public function getConnectionReferenceName(): string
    {
        return $this->connectionReferenceName;
    }
}