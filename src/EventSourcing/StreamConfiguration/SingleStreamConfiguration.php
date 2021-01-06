<?php


namespace Ecotone\EventSourcing\StreamConfiguration;


use Ecotone\EventSourcing\StreamConfiguration;
use Prooph\EventStore\StreamName;

class SingleStreamConfiguration implements StreamConfiguration
{
    const DEFAULT_STREAM_TABLE = 'event_stream';
    private string $streamName;

    public function __construct(string $streamName)
    {
        $this->streamName = $streamName;
    }

    public function generate(string $aggregateClassName, string $aggregateId): StreamName
    {
        return new StreamName($this->streamName);
    }

    public function isOneStreamPerAggregate(): bool
    {
        return false;
    }
}