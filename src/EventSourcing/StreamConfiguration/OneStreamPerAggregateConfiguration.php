<?php

namespace Ecotone\EventSourcing\StreamConfiguration;

use Ecotone\EventSourcing\StreamConfiguration;
use Prooph\EventStore\StreamName;

class OneStreamPerAggregateConfiguration implements StreamConfiguration
{
    public function generate(string $aggregateClassName, string $aggregateId): StreamName
    {
        return new StreamName($aggregateClassName . "-" . $aggregateId);
    }

    public function isOneStreamPerAggregate(): bool
    {
        return false;
    }
}