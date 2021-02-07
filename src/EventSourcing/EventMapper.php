<?php


namespace Ecotone\EventSourcing;


use DateTimeImmutable;
use DateTimeZone;
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
            new DateTimeImmutable($messageData['created_at'], new DateTimeZone('UTC')),
            $messageData['payload'],
            $messageData['metadata'],
            $eventType
        );
    }

    public function mapEventToName(object $event): string
    {
        $className = get_class($event);
        if (array_key_exists($className, $this->eventToNameMapping)) {
            return $this->eventToNameMapping[$className];
        }

        return $className;
    }
}