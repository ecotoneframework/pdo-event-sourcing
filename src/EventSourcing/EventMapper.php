<?php


namespace Ecotone\EventSourcing;


use DateTimeImmutable;
use DateTimeZone;
use Ecotone\Messaging\Handler\TypeDescriptor;
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageFactory;
use Ramsey\Uuid\Uuid;

class EventMapper implements MessageFactory
{
    private array $eventToNameMapping;
    private array $nameToEventMapping;

    private function __construct(array $eventToNameMapping, array $nameToEventMapping)
    {
        $this->eventToNameMapping = $eventToNameMapping;
        $this->nameToEventMapping = $nameToEventMapping;
    }

    public static function createEmpty() : self
    {
        return new self([],[]);
    }

    public function createMessageFromArray(string $messageName, array $messageData): Message
    {
        $eventType = $messageName;
        if (array_key_exists($messageName, $this->nameToEventMapping)) {
            $eventType = $this->nameToEventMapping[$messageName];
        }

        return new ProophEvent(
            Uuid::fromString($messageData['uuid']),
            $messageData['created_at'],
            $messageData['payload'],
            $messageData['metadata'],
            $eventType
        );
    }

    public function mapNameToEventType(string $name) : string
    {
        if ($name === TypeDescriptor::ARRAY) {
            return TypeDescriptor::ARRAY;
        }

        if (array_key_exists($name, $this->nameToEventMapping)) {
            return $this->nameToEventMapping[$name];
        }

        return $name;
    }

    public function mapEventToName(object|array $event): string
    {
        if (is_array($event)) {
            return TypeDescriptor::ARRAY;
        }

        $className = get_class($event);
        if (array_key_exists($className, $this->eventToNameMapping)) {
            return $this->eventToNameMapping[$className];
        }

        return $className;
    }
}