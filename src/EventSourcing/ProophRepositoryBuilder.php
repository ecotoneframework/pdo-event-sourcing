<?php


namespace Ecotone\EventSourcing;


use Ecotone\EventSourcing\StreamConfiguration\OneStreamPerAggregateConfiguration;
use Ecotone\EventSourcing\StreamConfiguration\SingleStreamConfiguration;
use Ecotone\Lite\InMemoryPSRContainer;
use Ecotone\Messaging\Conversion\ConversionService;
use Ecotone\Messaging\Handler\ChannelResolver;
use Ecotone\Messaging\Handler\Processor\MethodInvoker\Converter\ReferenceBuilder;
use Ecotone\Messaging\Handler\ReferenceSearchService;
use Ecotone\Messaging\MessageConverter\DefaultHeaderMapper;
use Ecotone\Messaging\MessageConverter\HeaderMapper;
use Ecotone\Modelling\EventSourcedRepository;
use Ecotone\Modelling\RepositoryBuilder;
use Ecotone\Modelling\StandardRepository;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Pdo\Container\AbstractEventStoreFactory;
use Prooph\EventStore\Pdo\MariaDbEventStore;
use Prooph\EventStore\Pdo\MySqlEventStore;
use Prooph\EventStore\Pdo\PersistenceStrategy;
use Prooph\EventStore\Pdo\PostgresEventStore;
use Prooph\EventStore\Pdo\WriteLockStrategy\MariaDbMetadataLockStrategy;
use Prooph\EventStore\Pdo\WriteLockStrategy\MysqlMetadataLockStrategy;
use Prooph\EventStore\Pdo\WriteLockStrategy\NoLockStrategy;
use Prooph\EventStore\Pdo\WriteLockStrategy\PostgresAdvisoryLockStrategy;
use Prooph\EventStore\StreamName;

class ProophRepositoryBuilder implements RepositoryBuilder
{
    const EVENT_STORE_TYPE_MYSQL = "mysql";
    const EVENT_STORE_TYPE_POSTGRES = "postgres";
    const EVENT_STORE_TYPE_MARIADB = "mariadb";

    private const SINGLE_STREAM_PERSISTENCE = "single";
    private const AGGREGATE_STREAM_PERSISTENCE = "aggregate";

    private string $eventStoreType;
    private ?ProophEventConverter $eventConverter;
    private string $streamPersistenceStrategy = self::SINGLE_STREAM_PERSISTENCE;
    /** @var int How many event should we returned on one call */
    private int $loadBatchSize = 1000;
    private array $handledAggregateClassNames;
    private array $headerMapper = [];
    private bool $enableWriteLockStrategy = false;
    private string $eventStreamTable = SingleStreamConfiguration::DEFAULT_STREAM_TABLE;

    private function __construct() {}

    /**
     * @param string $eventStoreType mysql|postgres|mariadb
     */
    public static function create(string $eventStoreType) : static
    {
        $self = new static();

        $self->eventStoreType = $eventStoreType;

        return $self;
    }

    public function canHandle(string $aggregateClassName): bool
    {

    }

    public function withMetadataMapper(string $headerMapper): self
    {
        $this->headerMapper = explode(",", $headerMapper);

        return $this;
    }

    public function build(ChannelResolver $channelResolver, ReferenceSearchService $referenceSearchService) : EventSourcedRepository
    {
        /** @var ConversionService $conversionService */
        $conversionService = $referenceSearchService->get(ConversionService::REFERENCE_NAME);
        $headerMapper = DefaultHeaderMapper::createAllHeadersMapping($conversionService);
        if ($this->headerMapper) {
            $headerMapper = DefaultHeaderMapper::createWith($this->headerMapper, $this->headerMapper, $conversionService);
        }

        $connection = null; // what to do with connection. Require ecotone/dbal and do wrapper http://dan.doezema.com/2015/08/doctrine-2-pdo-object/?
        $writeLockStrategy = new NoLockStrategy();
        if ($this->enableWriteLockStrategy) {
            $writeLockStrategy = match ($this->eventStoreType) {
                self::EVENT_STORE_TYPE_MYSQL =>  new MysqlMetadataLockStrategy($connection),
                self::EVENT_STORE_TYPE_MARIADB => new MariaDbMetadataLockStrategy($connection),
                self::EVENT_STORE_TYPE_POSTGRES => new PostgresAdvisoryLockStrategy($connection),
                default => throw new \Exception('Unexpected match value ' . $this->eventStoreType)
            };
        }

        $persistenceStrategy = null;

        $persistenceStrategy = match ($this->eventStoreType) {
            self::EVENT_STORE_TYPE_MYSQL => $this->getMysqlPersistenceStrategy(),
            self::EVENT_STORE_TYPE_MARIADB => $this->getMeriaPersistenceStrategy(),
            self::EVENT_STORE_TYPE_POSTGRES => $this->getPostgresPersistenceStrategy()
        };

        $eventStoreClass = match($this->eventStoreType) {
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
            $this->handledAggregateClassNames,
            $eventStore,
            $this->streamPersistenceStrategy === self::SINGLE_STREAM_PERSISTENCE ? new SingleStreamConfiguration($this->eventStreamTable) : new OneStreamPerAggregateConfiguration(),
            $headerMapper,
            $referenceSearchService->get(EventMapper::class),
            $conversionService
        );
    }

    private function getMysqlPersistenceStrategy() : PersistenceStrategy
    {
        return match ($this->streamPersistenceStrategy) {
            self::AGGREGATE_STREAM_PERSISTENCE => new PersistenceStrategy\MySqlAggregateStreamStrategy($this->eventConverter),
            self::SINGLE_STREAM_PERSISTENCE => new PersistenceStrategy\MySqlSingleStreamStrategy($this->eventConverter)
        };
    }

    private function getPostgresPersistenceStrategy() : PersistenceStrategy
    {
        return match ($this->streamPersistenceStrategy) {
            self::AGGREGATE_STREAM_PERSISTENCE => new PersistenceStrategy\PostgresAggregateStreamStrategy($this->eventConverter),
            self::SINGLE_STREAM_PERSISTENCE => new PersistenceStrategy\PostgresSingleStreamStrategy($this->eventConverter)
        };
    }

    private function getMeriaPersistenceStrategy() : PersistenceStrategy
    {
        return match ($this->streamPersistenceStrategy) {
            self::AGGREGATE_STREAM_PERSISTENCE => new PersistenceStrategy\MariaDbAggregateStreamStrategy($this->eventConverter),
            self::SINGLE_STREAM_PERSISTENCE => new PersistenceStrategy\MariaDbSingleStreamStrategy($this->eventConverter)
        };
    }
}