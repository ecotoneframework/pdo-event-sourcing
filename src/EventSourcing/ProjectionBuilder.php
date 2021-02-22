<?php


namespace Ecotone\EventSourcing;


use Ecotone\Messaging\Conversion\ConversionService;
use Ecotone\Messaging\Handler\ChannelResolver;
use Ecotone\Messaging\Handler\ReferenceSearchService;
use Ecotone\Messaging\MessageConverter\DefaultHeaderMapper;
use Ecotone\Modelling\EventSourcedRepository;
use Enqueue\Dbal\DbalConnectionFactory;

class ProjectionBuilder
{
    private bool $initializeTablesOnStart = true;
    private ProophEventConverter $eventConverter;
    /** @var int How many event should we returned on one call */
    private int $loadBatchSize = 1000;
    private array $handledAggregateClassNames = [];
    private array $headerMapper = [];
    private bool $enableWriteLockStrategy = false;
    private string $eventStreamTable = LazyEventStore::DEFAULT_STREAM_TABLE;
    private string $projectionsTable = LazyEventStore::DEFAULT_PROJECTIONS_TABLE;
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

        $eventStore = new LazyEventStore(
            $this->initializeTablesOnStart,
            $referenceSearchService->get(EventMapper::class),
            $this->eventConverter,
            $referenceSearchService,
            $this->connectionReferenceName,
            LazyEventStore::AGGREGATE_STREAM_PERSISTENCE,
            $this->enableWriteLockStrategy,
            $this->eventStreamTable,
            $this->projectionsTable,
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
}