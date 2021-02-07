<?php


namespace Ecotone\EventSourcing\StreamConfiguration;


use Ecotone\EventSourcing\StreamConfiguration;
use Prooph\EventStore\StreamName;

class SingleStreamConfiguration implements StreamConfiguration
{
    private string $streamName;

    public function __construct(string $streamName)
    {
        $this->streamName = $streamName;
    }

    public function generate(string $aggregateClassName, string $aggregateId): StreamName
    {
        return new StreamName("event_stream");
    }

    public function isOneStreamPerAggregate(): bool
    {
        return false;
    }
}