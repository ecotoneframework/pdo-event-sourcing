<?php


namespace Ecotone\EventSourcing;

use Prooph\EventStore\StreamName;

interface StreamConfiguration
{
    public function generate(string $aggregateClassName, string $aggregateId) : StreamName;

    public function isOneStreamPerAggregate() : bool;
}