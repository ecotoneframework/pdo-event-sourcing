<?php

namespace Ecotone\EventSourcing;

use DateTimeImmutable;
use DateTimeZone;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\PDOConnection;
use Ecotone\EventSourcing\StreamConfiguration\SingleStreamConfiguration;
use Ecotone\Messaging\Conversion\ConversionService;
use Ecotone\Messaging\Conversion\MediaType;
use Ecotone\Messaging\Handler\ClassDefinition;
use Ecotone\Messaging\Handler\TypeDescriptor;
use Ecotone\Messaging\MessageConverter\HeaderMapper;
use Ecotone\Messaging\MessageHeaders;
use Ecotone\Modelling\EventSourcedRepository;
use Ecotone\Modelling\EventStream;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Metadata\Operator;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use Ramsey\Uuid\Uuid;

class ProophRepository implements EventSourcedRepository
{
    const AGGREGATE_VERSION = '_aggregate_version';
    const AGGREGATE_TYPE = '_aggregate_type';
    const AGGREGATE_ID = '_aggregate_id';

    private HeaderMapper $headerMapper;
    private array $handledAggregateClassNames;
    private string $eventStreamTable;
    private array $aggregateClassToStreamName;
    private ProophEventStoreWrapper $eventStore;

    public function __construct(ProophEventStoreWrapper $eventStore, string $eventStreamTable, array $handledAggregateClassNames, HeaderMapper $headerMapper, array $aggregateClassStreamNames)
    {
        $this->eventStore = $eventStore;
        $this->headerMapper = $headerMapper;
        $this->handledAggregateClassNames = $handledAggregateClassNames;
        $this->eventStreamTable = $eventStreamTable;
        $this->aggregateClassToStreamName = $aggregateClassStreamNames;
    }

    public function canHandle(string $aggregateClassName): bool
    {
        return in_array($aggregateClassName, $this->handledAggregateClassNames);
    }

    public function findBy(string $aggregateClassName, array $identifiers): EventStream
    {
        $aggregateId = reset($identifiers);
        $streamName = $this->getStreamName($aggregateClassName, $aggregateId);

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            self::AGGREGATE_TYPE,
            Operator::EQUALS(),
            $aggregateClassName
        );
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            self::AGGREGATE_ID,
            Operator::EQUALS(),
            $aggregateId
        );

        try {
            $streamEvents = $this->eventStore->load($streamName, 1, null, $metadataMatcher);
        } catch (StreamNotFound) { return EventStream::createEmpty(); }

        $aggregateVersion = 0;
        if (!empty($streamEvents)) {
            $aggregateVersion = $streamEvents[array_key_last($streamEvents)]->getMetadata()[self::AGGREGATE_VERSION];
        }

        return EventStream::createWith($aggregateVersion, $streamEvents);
    }

    public function save(array $identifiers, string $aggregateClassName, array $events, array $metadata, int $versionBeforeHandling): void
    {
        $aggregateId = reset($identifiers);
        $streamName = $this->getStreamName($aggregateClassName, $aggregateId);

        $eventsWithMetadata = [];
        $eventsCount = count($events);
        for ($eventNumber = 1; $eventNumber <= $eventsCount; $eventNumber++) {
            $event = $events[$eventNumber - 1];
            $eventsWithMetadata[] = EventWithMetadata::create(
                $event,
                array_merge(
                    $this->headerMapper->mapFromMessageHeaders($metadata),
                    [
                        self::AGGREGATE_ID => $aggregateId,
                        self::AGGREGATE_TYPE => $aggregateClassName,
                        self::AGGREGATE_VERSION => $versionBeforeHandling + $eventNumber
                    ]
                )
            );
        }
        $this->eventStore->appendTo($streamName, $eventsWithMetadata);
    }

    private function getStreamName(string $aggregateClassName, mixed $aggregateId): StreamName
    {
        $prefix = $aggregateClassName;
        if (array_key_exists($aggregateClassName, $this->aggregateClassToStreamName)) {
            $prefix =  $this->aggregateClassToStreamName[$aggregateClassName];
        }

        return new StreamName($prefix . "-" . $aggregateId);
    }
}