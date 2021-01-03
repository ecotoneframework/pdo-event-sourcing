<?php


namespace Ecotone\EventSourcing;


use DateTimeImmutable;
use DateTimeZone;
use Ecotone\Messaging\Conversion\ConversionService;
use Ecotone\Messaging\Conversion\MediaType;
use Ecotone\Messaging\Handler\TypeDescriptor;
use Ecotone\Messaging\MessageConverter\HeaderMapper;
use Ecotone\Messaging\MessageHeaders;
use Ecotone\Modelling\EventSourcedRepository;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Metadata\Operator;
use Prooph\EventStore\Stream;
use Ramsey\Uuid\Uuid;

class ProophRepository implements EventSourcedRepository
{
    private EventStore $eventStore;
    private StreamConfiguration $streamConfiguration;
    private HeaderMapper $headerMapper;
    private array $handledAggregateClassNames;
    private EventMapper $eventMapper;
    private ConversionService $conversionService;

    public function __construct(array $handledAggregateClassNames, EventStore $eventStore, StreamConfiguration $streamConfiguration, HeaderMapper $headerMapper, EventMapper $eventMapper, ConversionService $conversionService)
    {
        $this->eventStore = $eventStore;
        $this->streamConfiguration = $streamConfiguration;
        $this->headerMapper = $headerMapper;
        $this->handledAggregateClassNames = $handledAggregateClassNames;
        $this->eventMapper = $eventMapper;
        $this->conversionService = $conversionService;
    }

    public function canHandle(string $aggregateClassName): bool
    {
        return in_array($aggregateClassName, $this->handledAggregateClassNames);
    }

    public function findBy(string $aggregateClassName, array $identifiers): ?array
    {
        $aggregateId = reset($identifiers);
        $streamName = $this->streamConfiguration->generate($aggregateClassName, $aggregateId);

        if ($this->streamConfiguration->isOneStreamPerAggregate()) {
            try {
                $streamEvents = $this->eventStore->load($streamName, 1);
            } catch (StreamNotFound $e) {
                return null;
            }
        } else {
            $metadataMatcher = new MetadataMatcher();
            $metadataMatcher = $metadataMatcher->withMetadataMatch(
                '_aggregate_type',
                Operator::EQUALS(),
                $aggregateClassName
            );
            $metadataMatcher = $metadataMatcher->withMetadataMatch(
                '_aggregate_id',
                Operator::EQUALS(),
                $aggregateId
            );

            try {
                $streamEvents = $this->eventStore->load($streamName, 1, null, $metadataMatcher);
            } catch (StreamNotFound $e) {
                return null;
            }
        }

        if (!$streamEvents->valid()) {
            return null;
        }

        return $streamEvents;
    }

    public function save(array $identifiers, string $aggregateClassName, array $events, array $metadata, ?int $expectedVersion): void
    {
        $aggregateId = reset($identifiers);
        $streamName = $this->streamConfiguration->generate($aggregateClassName, $aggregateId);

        $proophEvents = [];
        foreach ($events as $eventToConvert) {
            $proophEvents[] = new ProophEvent(
                Uuid::fromString($metadata[MessageHeaders::MESSAGE_ID]),
                new DateTimeImmutable("@" . $metadata[MessageHeaders::TIMESTAMP], new DateTimeZone('UTC')),
                $this->conversionService->convert($eventToConvert, TypeDescriptor::createFromVariable($eventToConvert), MediaType::createApplicationXPHP(), TypeDescriptor::createArrayType(), MediaType::createApplicationXPHP()),
                array_merge(
                    $this->headerMapper->mapFromMessageHeaders($metadata),
                    [
                        '_aggregate_id' => $aggregateId,
                        '_aggregate_type' => $aggregateClassName,
                        '_aggregate_version' => $expectedVersion // @TODO
                    ]
                ),
                $this->eventMapper->mapEventToName($eventToConvert)
            );
        }

        if (!$this->eventStore->hasStream($streamName)) {
            $this->eventStore->create(new Stream($streamName, new ArrayIterator($proophEvents), []));
        } else {
            $this->eventStore->appendTo($streamName, new ArrayIterator($proophEvents));
        }
    }
}