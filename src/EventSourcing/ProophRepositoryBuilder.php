<?php


namespace Ecotone\EventSourcing;


use Doctrine\DBAL\Driver\PDOConnection;
use Ecotone\Dbal\DbalReconnectableConnectionFactory;
use Ecotone\EventSourcing\StreamConfiguration\OneStreamPerAggregateInstanceConfiguration;
use Ecotone\EventSourcing\StreamConfiguration\SingleStreamConfiguration;
use Ecotone\Messaging\Conversion\ConversionService;
use Ecotone\Messaging\Handler\ChannelResolver;
use Ecotone\Messaging\Handler\ReferenceSearchService;
use Ecotone\Messaging\MessageConverter\DefaultHeaderMapper;
use Ecotone\Modelling\EventSourcedRepository;
use Ecotone\Modelling\RepositoryBuilder;
use Enqueue\Dbal\DbalConnectionFactory;
use Exception;
use Prooph\EventStore\Pdo\MariaDbEventStore;
use Prooph\EventStore\Pdo\MySqlEventStore;
use Prooph\EventStore\Pdo\PersistenceStrategy;
use Prooph\EventStore\Pdo\PostgresEventStore;
use Prooph\EventStore\Pdo\WriteLockStrategy\MariaDbMetadataLockStrategy;
use Prooph\EventStore\Pdo\WriteLockStrategy\MysqlMetadataLockStrategy;
use Prooph\EventStore\Pdo\WriteLockStrategy\NoLockStrategy;
use Prooph\EventStore\Pdo\WriteLockStrategy\PostgresAdvisoryLockStrategy;

class ProophRepositoryBuilder implements RepositoryBuilder
{
    const EVENT_STORE_TYPE_MYSQL = "mysql";
    const EVENT_STORE_TYPE_POSTGRES = "postgres";
    const EVENT_STORE_TYPE_MARIADB = "mariadb";

    private const SINGLE_STREAM_PERSISTENCE = "single";
    private const AGGREGATE_STREAM_PERSISTENCE = "aggregate";

    private ProophEventConverter $eventConverter;
    private string $streamPersistenceStrategy = self::AGGREGATE_STREAM_PERSISTENCE;
    /** @var int How many event should we returned on one call */
    private int $loadBatchSize = 1000;
    private array $handledAggregateClassNames = [];
    private array $headerMapper = [];
    private bool $enableWriteLockStrategy = false;
    private string $eventStreamTable = ProophRepository::STREAM_TABLE;
    private string $connectionReferenceName;
    private array $aggregateClassToStreamName = [];

    private function __construct(string $connectionReferenceName)
    {
        $this->connectionReferenceName = $connectionReferenceName;
        $this->eventConverter = new ProophEventConverter();
    }

    public static function create(string $connectionReferenceName = DbalConnectionFactory::class): static
    {
        return new static($connectionReferenceName);
    }

    public function canHandle(string $aggregateClassName): bool
    {
        return in_array($aggregateClassName, $this->handledAggregateClassNames);
    }

    public function withAggregateClassesToHandle(array $aggregateClassesToHandle) : self
    {
        $this->handledAggregateClassNames = $aggregateClassesToHandle;

        return $this;
    }

    public function isEventSourced(): bool
    {
        return true;
    }

    public function withMetadataMapper(string $headerMapper): self
    {
        $this->headerMapper = explode(",", $headerMapper);

        return $this;
    }

    public function withAggregateClassToStreamMapping(array $aggregateClassToStreamName) : static
    {
        $this->aggregateClassToStreamName = $aggregateClassToStreamName;

        return $this;
    }

    public function build(ChannelResolver $channelResolver, ReferenceSearchService $referenceSearchService): EventSourcedRepository
    {
        /** @var ConversionService $conversionService */
        $conversionService = $referenceSearchService->get(ConversionService::REFERENCE_NAME);
        $headerMapper = DefaultHeaderMapper::createAllHeadersMapping($conversionService);
        if ($this->headerMapper) {
            $headerMapper = DefaultHeaderMapper::createWith($this->headerMapper, $this->headerMapper, $conversionService);
        }

        $connectionFactory = new DbalReconnectableConnectionFactory($referenceSearchService->get($this->connectionReferenceName)); // what to do with connection. Require ecotone/dbal and do wrapper http://dan.doezema.com/2015/08/doctrine-2-pdo-object/?

        /** @var PDOConnection $connection */
        $connection = $connectionFactory->getConnection()->getWrappedConnection();

        $eventStoreType = $connection->getAttribute(\PDO::ATTR_DRIVER_NAME);
        if ($eventStoreType === self::EVENT_STORE_TYPE_MYSQL && str_contains($connection->getAttribute(\PDO::ATTR_SERVER_VERSION), "MariaDB")) {
            $eventStoreType = self::EVENT_STORE_TYPE_MARIADB;
        }
        if ($eventStoreType === "pgsql") {
            $eventStoreType = self::EVENT_STORE_TYPE_POSTGRES;
        }

        $persistenceStrategy = match ($eventStoreType) {
            self::EVENT_STORE_TYPE_MYSQL => $this->getMysqlPersistenceStrategy(),
            self::EVENT_STORE_TYPE_MARIADB => $this->getMeriaPersistenceStrategy(),
            self::EVENT_STORE_TYPE_POSTGRES => $this->getPostgresPersistenceStrategy(),
            default => throw new Exception('Unexpected match value ' . $eventStoreType)
        };

        $writeLockStrategy = new NoLockStrategy();
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
            $referenceSearchService->get(EventMapper::class),
            $connection,
            $persistenceStrategy,
            $this->loadBatchSize,
            $this->eventStreamTable,
            true,
            $writeLockStrategy
        );

        return new ProophRepository(
            $eventStoreType,
            $this->eventStreamTable,
            $this->handledAggregateClassNames,
            $eventStore,
            $headerMapper,
            $referenceSearchService->get(EventMapper::class),
            $conversionService,
            $connectionFactory->getConnection(),
            []
        );
    }

    private function getMysqlPersistenceStrategy(): PersistenceStrategy
    {
        return match ($this->streamPersistenceStrategy) {
            self::AGGREGATE_STREAM_PERSISTENCE => new PersistenceStrategy\MySqlAggregateStreamStrategy($this->eventConverter),
            self::SINGLE_STREAM_PERSISTENCE => new PersistenceStrategy\MySqlSingleStreamStrategy($this->eventConverter)
        };
    }

    private function getMeriaPersistenceStrategy(): PersistenceStrategy
    {
        return match ($this->streamPersistenceStrategy) {
            self::AGGREGATE_STREAM_PERSISTENCE => new PersistenceStrategy\MariaDbAggregateStreamStrategy($this->eventConverter),
            self::SINGLE_STREAM_PERSISTENCE => new PersistenceStrategy\MariaDbSingleStreamStrategy($this->eventConverter)
        };
    }

    private function getPostgresPersistenceStrategy(): PersistenceStrategy
    {
        return match ($this->streamPersistenceStrategy) {
            self::AGGREGATE_STREAM_PERSISTENCE => new PersistenceStrategy\PostgresAggregateStreamStrategy($this->eventConverter),
            self::SINGLE_STREAM_PERSISTENCE => new PersistenceStrategy\PostgresSingleStreamStrategy($this->eventConverter)
        };
    }
}