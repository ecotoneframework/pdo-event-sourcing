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

class ProophRepositoryBuilder
{
    private const SINGLE_STREAM_PERSISTENCE = "single";
    private const AGGREGATE_STREAM_PERSISTENCE = "aggregate";

    /** @var string|EventStore EventStore reference */
    private string|EventStore $eventStore;
    private ?ProophEventConverter $eventConverter;
    private string $streamPersistenceStrategy = self::SINGLE_STREAM_PERSISTENCE;
    /** @var int How many event should we returned on one call */
    private int $loadBatchSize = 1000;
    private array $handledAggregateClassNames;
    private EventMapper $eventMapper;
    private array $headerMapper = [];
    private bool $enableWriteLockStrategy = false;
    private string $eventStreamTable = SingleStreamConfiguration::DEFAULT_STREAM_TABLE;

    public static function create(string $eventStoreReference, string $streamPersistenceStrategy, int $loadBatchSize) : static
    {

    }

    public static function createWithEventStore(EventStore $eventStore) : static
    {

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

        if ($this->eventStore instanceof EventStore) {
            $eventStore = $this->eventStore;
        }else {
            $connection = null;
            $writeLockStrategy = new NoLockStrategy();
            if ($this->enableWriteLockStrategy) {
                $writeLockStrategy = match ($this->eventStore) {
                    MySqlEventStore::class =>  new MysqlMetadataLockStrategy($connection),
                    MariaDbEventStore::class => new MariaDbMetadataLockStrategy($connection),
                    PostgresEventStore::class => new PostgresAdvisoryLockStrategy($connection),
                    default => throw new \Exception('Unexpected match value ' . $this->eventStore)
                };
            }

            $persistenceStrategy = null;

            $persistenceStrategy = match ($this->eventStore) {
                MySqlEventStore::class => $this->getMysqlPersistenceStrategy(),
                MariaDbEventStore::class => $this->getMeriaPersistenceStrategy(),
                PostgresEventStore::class => $this->getPostgresPersistenceStrategy()
            };

            $eventStore =  $eventStore = new $this->eventStore(
                $this->eventMapper,
                $connection,
                $persistenceStrategy,
                $this->loadBatchSize,
                $this->eventStreamTable,
                true,
                $writeLockStrategy
        );
        }

        return new ProophRepository(
            $this->handledAggregateClassNames,
            $eventStore,
            $this->streamPersistenceStrategy === self::SINGLE_STREAM_PERSISTENCE ? new SingleStreamConfiguration() : new OneStreamPerAggregateConfiguration(),
            $headerMapper,
            $this->eventMapper,
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