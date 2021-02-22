<?php


namespace Ecotone\EventSourcing;


class EventWithMetadata
{
    private string $eventType;
    private $event;
    private array $metadata;

    private function __construct(string $eventType, $event, array $metadata)
    {
        $this->eventType = $eventType;
        $this->event = $event;
        $this->metadata = $metadata;
    }

    public static function create(object $event, array $metadata = [])
    {
        return new self(get_class($event), $event, $metadata);
    }

    public static function createWithType(string $eventType, array|object $event, array $metadata)
    {
        return new self($eventType, $event, $metadata);
    }

    public function getEventType(): string
    {
        return $this->eventType;
    }

    public function getEvent()
    {
        return $this->event;
    }

    public function getMetadata(): array
    {
        return $this->metadata;
    }
}