<?php


namespace Ecotone\EventSourcing\StreamConfiguration;


use Ecotone\EventSourcing\StreamConfiguration;
use Prooph\EventStore\StreamName;

class SingleStreamConfiguration implements StreamConfiguration
{
    const DEFAULT_STREAM_TABLE = 'event_stream';

    public function generate(string $aggregateClassName, string $aggregateId): StreamName
    {
        return new StreamName(self::DEFAULT_STREAM_TABLE);
    }

    public function isOneStreamPerAggregate(): bool
    {
        return false;
    }
}