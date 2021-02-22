<?php


namespace Ecotone\EventSourcing;

use Ecotone\Messaging\Conversion\ConversionService;
use Ecotone\Messaging\Conversion\InMemoryConversionService;
use Ecotone\Messaging\Conversion\MediaType;
use Ecotone\Messaging\Handler\TypeDescriptor;
use Ecotone\Messaging\MessageConverter\HeaderMapper;
use Ecotone\Messaging\MessageHeaders;
use Ecotone\Modelling\EventStream;
use Prooph\EventStore\EventStore as ProophEventStore;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use Ramsey\Uuid\Uuid;

class ProophEventStoreWrapper implements EventStore
{
    private ProophEventStore $eventStore;
    private ConversionService $conversionService;
    /**
     * @var EventMapper
     */
    private EventMapper $eventMapper;

    private function __construct(ProophEventStore $eventStore, ConversionService $conversionService, EventMapper $eventMapper)
    {
        $this->eventStore = $eventStore;
        $this->conversionService = $conversionService;
        $this->eventMapper = $eventMapper;
    }

    public static function prepare(ProophEventStore $eventStore, ConversionService $conversionService, EventMapper $eventMapper) : static
    {
        return new self($eventStore , $conversionService, $eventMapper);
    }

    public static function prepareWithNoConversions(ProophEventStore $eventStore) : static
    {
        return new self($eventStore, InMemoryConversionService::createWithoutConversion(), EventMapper::createEmpty());
    }

    /**
     * @inheritDoc
     */
    public function updateStreamMetadata(string $streamName, array $newMetadata): void
    {
        $this->eventStore->updateStreamMetadata(new StreamName($streamName), $newMetadata);
    }

    /**
     * @inheritDoc
     */
    public function create(string $streamName, array $events, array $streamMetadata): void
    {
        $this->eventStore->create(new Stream(new StreamName($streamName), $this->convertProophEvents($events), $streamMetadata));
    }

    public function appendTo(string $streamName, array $events): void
    {
        $this->eventStore->appendTo(new StreamName($streamName), $this->convertProophEvents($events));
    }

    public function delete(string $streamName): void
    {
        $this->eventStore->delete(new StreamName($streamName));
    }

    public function fetchStreamMetadata(string $streamName): array
    {
        return $this->eventStore->fetchStreamMetadata(new StreamName($streamName));
    }

    public function hasStream(string $streamName): bool
    {
        return $this->eventStore->hasStream(new StreamName($streamName));
    }

    public function load(string $streamName, int $fromNumber = 1, int $count = null, MetadataMatcher $metadataMatcher = null, bool $deserialize = true): array
    {
        $streamEvents = $this->eventStore->load(new StreamName($streamName), $fromNumber, $count, $metadataMatcher);
        if (!$streamEvents->valid()) {
            $streamEvents = new \ArrayIterator([]);
        }

        return $this->convertToEcotoneEvents(
            $streamEvents,
            $deserialize
        );
    }

    public function loadReverse(string $streamName, int $fromNumber = null, int $count = null, MetadataMatcher $metadataMatcher = null, bool $deserialize = true): array
    {
        $streamEvents = $this->eventStore->loadReverse(new StreamName($streamName), $fromNumber, $count, $metadataMatcher);
        if (!$streamEvents->valid()) {
            $streamEvents = new \ArrayIterator([]);
        }

        return $this->convertToEcotoneEvents(
            $streamEvents,
            $deserialize
        );
    }

    public function fetchStreamNames(?string $filter, ?MetadataMatcher $metadataMatcher, int $limit = 20, int $offset = 0): array
    {
        return $this->eventStore->fetchStreamNames($filter, $metadataMatcher, $limit, $offset);
    }

    public function fetchStreamNamesRegex(string $filter, ?MetadataMatcher $metadataMatcher, int $limit = 20, int $offset = 0): array
    {
        return $this->eventStore->fetchStreamNamesRegex($filter, $metadataMatcher, $limit, $offset);
    }

    public function fetchCategoryNames(?string $filter, int $limit = 20, int $offset = 0): array
    {
        return $this->eventStore->fetchCategoryNames($filter, $limit, $offset);
    }

    public function fetchCategoryNamesRegex(string $filter, int $limit = 20, int $offset = 0): array
    {
        return $this->fetchCategoryNamesRegex($filter, $limit, $offset);
    }

    /**
     * @param EventWithMetadata[]|object[]|array[] $streamEvents
     */
    private function convertProophEvents(array $events): \ArrayIterator
    {
        $proophEvents = [];
        foreach ($events as $eventToConvert) {
            $payload = $eventToConvert;
            $metadata = [];
            if ($eventToConvert instanceof EventWithMetadata) {
                $payload = $eventToConvert->getEvent();
                $metadata = $eventToConvert->getMetadata();
            }

            $proophEvents[] = new ProophEvent(
                Uuid::fromString($metadata[MessageHeaders::MESSAGE_ID]),
                new \DateTimeImmutable("@" . $metadata[MessageHeaders::TIMESTAMP], new \DateTimeZone('UTC')),
                $this->conversionService->convert($payload, TypeDescriptor::createFromVariable($payload), MediaType::createApplicationXPHP(), TypeDescriptor::createArrayType(), MediaType::createApplicationXPHP()),
                $metadata,
                $this->eventMapper->mapEventToName($payload)
            );
        }

        return new \ArrayIterator($proophEvents);
    }

    /**
     * @return EventWithMetadata[]
     */
    private function convertToEcotoneEvents(\Iterator $streamEvents, bool $deserialize): array
    {
        $events = [];
        $sourcePHPType = TypeDescriptor::createArrayType();
        $PHPMediaType = MediaType::createApplicationXPHP();
        /** @var ProophEvent $event */
        while ($event = $streamEvents->current()) {
            $eventName = TypeDescriptor::create($this->eventMapper->mapNameToEventType($event->messageName()));
            $events[] = EventWithMetadata::createWithType(
                $eventName,
                $deserialize ? $this->conversionService->convert($event->payload(), $sourcePHPType, $PHPMediaType, $eventName, $PHPMediaType) : $event->payload(),
                $event->metadata()
            );

            $streamEvents->next();
        }

        return $events;
    }
}