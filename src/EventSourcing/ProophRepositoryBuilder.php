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
    private bool $initializeTablesOnStart = LazyProophEventStore::INITIALIZE_ON_STARTUP;
    /** @var int How many event should we returned on one call */
    private int $loadBatchSize = LazyProophEventStore::LOAD_BATCH_SIZE;
    private array $handledAggregateClassNames = [];
    private array $headerMapper = [];
    private bool $enableWriteLockStrategy = LazyProophEventStore::DEFAULT_ENABLE_WRITE_LOCK_STRATEGY;
    private string $eventStreamTable = LazyProophEventStore::DEFAULT_STREAM_TABLE;
    private string $projectionsTable = LazyProophEventStore::DEFAULT_PROJECTIONS_TABLE;
    private string $connectionReferenceName;
    private array $aggregateClassToStreamName = [];

    private function __construct(string $connectionReferenceName)
    {
        $this->connectionReferenceName = $connectionReferenceName;
    }

    public static function create(string $connectionReferenceName = LazyProophEventStore::DEFAULT_CONNECTION_FACTORY): static
    {
        return new static($connectionReferenceName);
    }

    public function withTableInitializationOnStartup(bool $initialize) : static
    {
        $this->initializeTablesOnStart = $initialize;

        return $this;
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

        $eventStore = new LazyProophEventStore(
            $this->initializeTablesOnStart,
            $referenceSearchService->get(EventMapper::class),
            $referenceSearchService,
            $this->connectionReferenceName,
            LazyProophEventStore::AGGREGATE_STREAM_PERSISTENCE,
            $this->enableWriteLockStrategy,
            $this->eventStreamTable,
            $this->projectionsTable,
            $this->loadBatchSize
        );

        return new ProophRepository(
            EventStoreProophIntegration::prepare($eventStore, $conversionService, $referenceSearchService->get(EventMapper::class)),
            $this->eventStreamTable,
            $this->handledAggregateClassNames,
            $headerMapper,
            []
        );
    }
}