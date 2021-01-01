<?php

namespace Ecotone\EventSourcing\StreamNameGenerator;

use Ecotone\EventSourcing\StreamNameGenerator;

class DefaultStreamNameGenerator implements StreamNameGenerator
{
    private array $streamAggregateMapping = [];

    public function __construct(array $streamAggregateMapping)
    {
        $this->streamAggregateMapping = $streamAggregateMapping;
    }

    public function generate(string $aggregateClassName, string $aggregateId): string
    {
        if (array_key_exists($aggregateClassName, $this->streamAggregateMapping)) {
            return $this->streamAggregateMapping[$aggregateClassName] . "-" . $aggregateId;
        }

        return $aggregateClassName . "-" . $aggregateId;
    }
}