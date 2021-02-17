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

        $eventStore = new LazyEventStore(
            $referenceSearchService->get(EventMapper::class),
            $this->eventConverter,
            $referenceSearchService,
            $this->connectionReferenceName,
            LazyEventStore::AGGREGATE_STREAM_PERSISTENCE,
            $this->enableWriteLockStrategy,
            $this->eventStreamTable,
            $this->loadBatchSize
        );

        return new ProophRepository(
            $this->eventStreamTable,
            $this->handledAggregateClassNames,
            $eventStore,
            $headerMapper,
            $referenceSearchService->get(EventMapper::class),
            $conversionService,
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