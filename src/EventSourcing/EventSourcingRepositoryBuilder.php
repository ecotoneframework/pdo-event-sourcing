<?php


namespace Ecotone\EventSourcing;


use Doctrine\DBAL\Driver\PDOConnection;
use Ecotone\Dbal\DbalReconnectableConnectionFactory;
use Ecotone\EventSourcing\Config\EventSourcingModule;
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

final class EventSourcingRepositoryBuilder implements RepositoryBuilder
{
    private array $handledAggregateClassNames = [];
    private array $headerMapper = [];
    private EventSourcingConfiguration $eventSourcingConfiguration;

    private function __construct(EventSourcingConfiguration $eventSourcingConfiguration)
    {
        $this->eventSourcingConfiguration = $eventSourcingConfiguration;
    }

    public static function create(EventSourcingConfiguration $eventSourcingConfiguration): static
    {
        return new static($eventSourcingConfiguration);
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

    public function withMetadataMapper(string $headerMapper): self
    {
        $this->headerMapper = explode(",", $headerMapper);

        return $this;
    }

    public function isEventSourced(): bool
    {
        return true;
    }

    public function build(ChannelResolver $channelResolver, ReferenceSearchService $referenceSearchService): EventSourcedRepository
    {
        /** @var ConversionService $conversionService */
        $conversionService = $referenceSearchService->get(ConversionService::REFERENCE_NAME);
        $headerMapper = DefaultHeaderMapper::createAllHeadersMapping($conversionService);
        if ($this->headerMapper) {
            $headerMapper = DefaultHeaderMapper::createWith($this->headerMapper, $this->headerMapper, $conversionService);
        }

        return new EventSourcingRepository(
            EcotoneEventStoreProophWrapper::prepare(
                new LazyProophEventStore($this->eventSourcingConfiguration, $referenceSearchService),
                $conversionService,
                $referenceSearchService->get(EventMapper::class)
            ),
            $this->handledAggregateClassNames,
            $headerMapper,
            $this->eventSourcingConfiguration,
            $referenceSearchService->get(AggregateStreamMapping::class)
        );
    }
}