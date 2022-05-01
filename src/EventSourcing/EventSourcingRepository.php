<?php

namespace Ecotone\EventSourcing;

use DateTimeImmutable;
use DateTimeZone;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\PDOConnection;
use Ecotone\Enqueue\OutboundMessageConverter;
use Ecotone\EventSourcing\StreamConfiguration\SingleStreamConfiguration;
use Ecotone\Messaging\Conversion\ConversionService;
use Ecotone\Messaging\Conversion\MediaType;
use Ecotone\Messaging\Handler\ClassDefinition;
use Ecotone\Messaging\Handler\Enricher\PropertyPath;
use Ecotone\Messaging\Handler\Enricher\PropertyReaderAccessor;
use Ecotone\Messaging\Handler\TypeDescriptor;
use Ecotone\Messaging\MessageConverter\HeaderMapper;
use Ecotone\Messaging\MessageHeaders;
use Ecotone\Messaging\Store\Document\DocumentException;
use Ecotone\Messaging\Store\Document\DocumentStore;
use Ecotone\Modelling\Attribute\AggregateVersion;
use Ecotone\Modelling\DistributedMetadata;
use Ecotone\Modelling\Event;
use Ecotone\Modelling\EventSourcedRepository;
use Ecotone\Modelling\EventStream;
use Ecotone\Modelling\SaveAggregateService;
use Ecotone\Modelling\SnapshotEvent;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Metadata\Operator;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use Ramsey\Uuid\Uuid;

class EventSourcingRepository implements EventSourcedRepository
{
    private HeaderMapper $headerMapper;
    private array $handledAggregateClassNames;
    private EcotoneEventStoreProophWrapper $eventStore;
    private EventSourcingConfiguration $eventSourcingConfiguration;
    private AggregateStreamMapping $aggregateStreamMapping;

    public function __construct(EcotoneEventStoreProophWrapper $eventStore, array $handledAggregateClassNames, HeaderMapper $headerMapper, EventSourcingConfiguration $eventSourcingConfiguration, AggregateStreamMapping $aggregateStreamMapping, private array $snapshotedAggregates, private DocumentStore $documentStore)
    {
        $this->eventStore = $eventStore;
        $this->headerMapper = $headerMapper;
        $this->handledAggregateClassNames = $handledAggregateClassNames;
        $this->eventSourcingConfiguration = $eventSourcingConfiguration;
        $this->aggregateStreamMapping = $aggregateStreamMapping;
    }

    public function canHandle(string $aggregateClassName): bool
    {
        return in_array($aggregateClassName, $this->handledAggregateClassNames);
    }

    public function findBy(string $aggregateClassName, array $identifiers): EventStream
    {
        $aggregateId = reset($identifiers);
        $aggregateVersion = 0;
        $streamName = $this->getStreamName($aggregateClassName, $aggregateId);
        $snapshotEvent = [];

        if (in_array($aggregateClassName, $this->snapshotedAggregates)) {
            try {
                $aggregate = $this->documentStore->getDocument(SaveAggregateService::SNAPSHOT_COLLECTION, $aggregateId);
                $propertyReader = new PropertyReaderAccessor();
                $versionAnnotation             = TypeDescriptor::create(AggregateVersion::class);
                $aggregateVersionPropertyName = null;
                foreach (ClassDefinition::createFor(TypeDescriptor::createFromVariable($aggregate))->getProperties() as $property) {
                    if ($property->hasAnnotation($versionAnnotation)) {
                        $aggregateVersionPropertyName = $property->getName();
                        break;
                    }
                }

                $aggregateVersion = $propertyReader->getPropertyValue(
                    PropertyPath::createWith($aggregateVersionPropertyName),
                    $aggregate
                );
                $snapshotEvent[] = new SnapshotEvent($aggregate);
            }catch (DocumentException) {}
        }

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            LazyProophEventStore::AGGREGATE_TYPE,
            Operator::EQUALS(),
            $aggregateClassName
        );
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            LazyProophEventStore::AGGREGATE_ID,
            Operator::EQUALS(),
            $aggregateId
        );

        try {
            $streamEvents = $this->eventStore->load($streamName, $aggregateVersion + 1, null, $metadataMatcher);
        } catch (StreamNotFound) { return EventStream::createEmpty(); }

        if (!empty($streamEvents)) {
            $aggregateVersion = $streamEvents[array_key_last($streamEvents)]->getMetadata()[LazyProophEventStore::AGGREGATE_VERSION];
        }

        return EventStream::createWith($aggregateVersion, array_merge($snapshotEvent, $streamEvents));
    }

    public function save(array $identifiers, string $aggregateClassName, array $events, array $metadata, int $versionBeforeHandling): void
    {
        $aggregateId = reset($identifiers);
        $streamName = $this->getStreamName($aggregateClassName, $aggregateId);
        $metadata = OutboundMessageConverter::unsetEnqueueMetadata($metadata);
        $metadata = DistributedMetadata::unsetDistributionKeys($metadata);

        $eventsWithMetadata = [];
        $eventsCount = count($events);
        for ($eventNumber = 1; $eventNumber <= $eventsCount; $eventNumber++) {
            $event = $events[$eventNumber - 1];
            $eventsWithMetadata[] = Event::create(
                $event,
                array_merge(
                    $this->headerMapper->mapFromMessageHeaders($metadata),
                    [
                        MessageHeaders::MESSAGE_ID => Uuid::uuid4()->toString(),
                        LazyProophEventStore::AGGREGATE_ID => $aggregateId,
                        LazyProophEventStore::AGGREGATE_TYPE => $aggregateClassName,
                        LazyProophEventStore::AGGREGATE_VERSION => $versionBeforeHandling + $eventNumber
                    ]
                )
            );
        }
        $this->eventStore->appendTo($streamName, $eventsWithMetadata);
    }

    private function getStreamName(string $aggregateClassName, mixed $aggregateId): StreamName
    {
        $streamName = $aggregateClassName;
        if (array_key_exists($aggregateClassName, $this->aggregateStreamMapping->getAggregateToStreamMapping())) {
            $streamName =  $this->aggregateStreamMapping->getAggregateToStreamMapping()[$aggregateClassName];
        }

        if ($this->eventSourcingConfiguration->isUsingAggregateStreamStrategy()) {
            $streamName = $streamName . "-" . $aggregateId;
        }

        return new StreamName($streamName);
    }
}