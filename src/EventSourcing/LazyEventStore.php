<?php


namespace Ecotone\EventSourcing;


use Doctrine\DBAL\Driver\PDOConnection;
use Ecotone\Dbal\DbalReconnectableConnectionFactory;
use Ecotone\Messaging\Handler\ReferenceSearchService;
use Ecotone\Messaging\Support\InvalidArgumentException;
use Iterator;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Pdo\MariaDbEventStore;
use Prooph\EventStore\Pdo\MySqlEventStore;
use Prooph\EventStore\Pdo\PdoEventStore;
use Prooph\EventStore\Pdo\PersistenceStrategy;
use Prooph\EventStore\Pdo\PostgresEventStore;
use Prooph\EventStore\Pdo\WriteLockStrategy\MariaDbMetadataLockStrategy;
use Prooph\EventStore\Pdo\WriteLockStrategy\MysqlMetadataLockStrategy;
use Prooph\EventStore\Pdo\WriteLockStrategy\NoLockStrategy;
use Prooph\EventStore\Pdo\WriteLockStrategy\PostgresAdvisoryLockStrategy;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;

class LazyEventStore implements PdoEventStore
{
    const EVENT_STORE_TYPE_MYSQL = "mysql";
    const EVENT_STORE_TYPE_POSTGRES = "postgres";
    const EVENT_STORE_TYPE_MARIADB = "mariadb";

    const SINGLE_STREAM_PERSISTENCE = "single";
    const AGGREGATE_STREAM_PERSISTENCE = "aggregate";

    private ?PdoEventStore $initializedEventStore = null;
    private ReferenceSearchService $referenceSearchService;
    private string $connectionReferenceName;
    private string $streamPersistenceStrategy;
    private bool $enableWriteLockStrategy;
    private string $eventStreamTable;
    private MessageFactory $messageFactory;
    private MessageConverter $messageConverter;
    private int $eventLoadBatchSize;

    public function __construct(MessageFactory $messageFactory, MessageConverter $messageConverter, ReferenceSearchService $referenceSearchService, string $connectionReferenceName, string $streamPersistenceStrategy, bool $enableWriteLockStrategy, string $eventStreamTable, int $eventLoadBatchSize)
    {
        $this->referenceSearchService = $referenceSearchService;
        $this->connectionReferenceName = $connectionReferenceName;
        $this->streamPersistenceStrategy = $streamPersistenceStrategy;
        $this->enableWriteLockStrategy = $enableWriteLockStrategy;
        $this->eventStreamTable = $eventStreamTable;
        $this->messageFactory = $messageFactory;
        $this->eventLoadBatchSize = $eventLoadBatchSize;
        $this->messageConverter = $messageConverter;
    }

    public function fetchStreamMetadata(StreamName $streamName): array
    {
        return $this->fetchStreamMetadata($streamName);
    }

    public function hasStream(StreamName $streamName): bool
    {
        return $this->getEventStore()->hasStream($streamName);
    }

    public function load(StreamName $streamName, int $fromNumber = 1, int $count = null, MetadataMatcher $metadataMatcher = null): Iterator
    {
        return $this->getEventStore()->load($streamName, $fromNumber, $count, $metadataMatcher);
    }

    public function loadReverse(StreamName $streamName, int $fromNumber = null, int $count = null, MetadataMatcher $metadataMatcher = null): Iterator
    {
        return $this->getEventStore()->loadReverse($streamName, $fromNumber, $count, $metadataMatcher);
    }

    public function fetchStreamNames(?string $filter, ?MetadataMatcher $metadataMatcher, int $limit = 20, int $offset = 0): array
    {
        return $this->getEventStore()->fetchStreamNames($filter, $metadataMatcher, $limit, $offset);
    }

    public function fetchStreamNamesRegex(string $filter, ?MetadataMatcher $metadataMatcher, int $limit = 20, int $offset = 0): array
    {
        return $this->getEventStore()->fetchStreamNamesRegex($filter, $metadataMatcher, $limit, $offset);
    }

    public function fetchCategoryNames(?string $filter, int $limit = 20, int $offset = 0): array
    {
        return $this->getEventStore()->fetchCategoryNames($filter, $limit, $offset);
    }

    public function fetchCategoryNamesRegex(string $filter, int $limit = 20, int $offset = 0): array
    {
        return $this->getEventStore()->fetchCategoryNamesRegex($filter, $limit, $offset);
    }

    public function updateStreamMetadata(StreamName $streamName, array $newMetadata): void
    {
        $this->getEventStore()->updateStreamMetadata($streamName, $newMetadata);
    }

    public function create(Stream $stream): void
    {
        $this->getEventStore()->create($stream);
    }

    public function appendTo(StreamName $streamName, Iterator $streamEvents): void
    {
        $this->getEventStore()->appendTo($streamName, $streamEvents);
    }

    public function delete(StreamName $streamName): void
    {
        $this->getEventStore()->delete($streamName);
    }

    private function getEventStore() : PdoEventStore
    {
        if ($this->initializedEventStore) {
            return $this->initializedEventStore;
        }

        $eventStoreType =  $this->getEventStoreType();

        $persistenceStrategy = match ($eventStoreType) {
            self::EVENT_STORE_TYPE_MYSQL => $this->getMysqlPersistenceStrategy(),
            self::EVENT_STORE_TYPE_MARIADB => $this->getMeriaPersistenceStrategy(),
            self::EVENT_STORE_TYPE_POSTGRES => $this->getPostgresPersistenceStrategy(),
            default => throw InvalidArgumentException::create('Unexpected match value ' . $eventStoreType)
        };

        $writeLockStrategy = new NoLockStrategy();
        $connection = $this->getWrappedConnection();
        if ($this->enableWriteLockStrategy) {
            $writeLockStrategy = match ($eventStoreType) {
                self::EVENT_STORE_TYPE_MYSQL => new MysqlMetadataLockStrategy($connection),
                self::EVENT_STORE_TYPE_MARIADB => new MariaDbMetadataLockStrategy($connection),
                self::EVENT_STORE_TYPE_POSTGRES => new PostgresAdvisoryLockStrategy($connection)
            };
        }

        $eventStoreClass = match ($eventStoreType) {
            self::EVENT_STORE_TYPE_MYSQL => MySqlEventStore::class,
            self::EVENT_STORE_TYPE_MARIADB => MariaDbEventStore::class,
            self::EVENT_STORE_TYPE_POSTGRES => PostgresEventStore::class
        };

        $eventStore = new $eventStoreClass(
            $this->messageFactory,
            $connection,
            $persistenceStrategy,
            $this->eventLoadBatchSize,
            $this->eventStreamTable,
            true,
            $writeLockStrategy
        );

        $this->initializedEventStore = $eventStore;

        return $eventStore;
    }

    private function getMysqlPersistenceStrategy(): PersistenceStrategy
    {
        return match ($this->streamPersistenceStrategy) {
            self::AGGREGATE_STREAM_PERSISTENCE => new PersistenceStrategy\MySqlAggregateStreamStrategy($this->messageConverter),
            self::SINGLE_STREAM_PERSISTENCE => new PersistenceStrategy\MySqlSingleStreamStrategy($this->messageConverter)
        };
    }

    private function getMeriaPersistenceStrategy(): PersistenceStrategy
    {
        return match ($this->streamPersistenceStrategy) {
            self::AGGREGATE_STREAM_PERSISTENCE => new PersistenceStrategy\MariaDbAggregateStreamStrategy($this->messageConverter),
            self::SINGLE_STREAM_PERSISTENCE => new PersistenceStrategy\MariaDbSingleStreamStrategy($this->messageConverter)
        };
    }

    private function getPostgresPersistenceStrategy(): PersistenceStrategy
    {
        return match ($this->streamPersistenceStrategy) {
            self::AGGREGATE_STREAM_PERSISTENCE => new PersistenceStrategy\PostgresAggregateStreamStrategy($this->messageConverter),
            self::SINGLE_STREAM_PERSISTENCE => new PersistenceStrategy\PostgresSingleStreamStrategy($this->messageConverter)
        };
    }

    public function getEventStoreType() : string
    {
        $connection = $this->getWrappedConnection();

        $eventStoreType = $connection->getAttribute(\PDO::ATTR_DRIVER_NAME);
        if ($eventStoreType === self::EVENT_STORE_TYPE_MYSQL && str_contains($connection->getAttribute(\PDO::ATTR_SERVER_VERSION), "MariaDB")) {
            $eventStoreType = self::EVENT_STORE_TYPE_MARIADB;
        }
        if ($eventStoreType === "pgsql") {
            $eventStoreType = self::EVENT_STORE_TYPE_POSTGRES;
        }
        return $eventStoreType;
    }

    public function getConnection(): \Doctrine\DBAL\Connection
    {
        $connectionFactory = new DbalReconnectableConnectionFactory($this->referenceSearchService->get($this->connectionReferenceName));

        return $connectionFactory->getConnection();
    }

    private function getWrappedConnection(): PDOConnection
    {
        return $this->getConnection()->getWrappedConnection();
    }
}