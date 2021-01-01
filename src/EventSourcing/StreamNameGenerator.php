<?php


namespace Ecotone\EventSourcing;

interface StreamNameGenerator
{
    public function generate(string $aggregateClassName, string $aggregateId) : string;
}