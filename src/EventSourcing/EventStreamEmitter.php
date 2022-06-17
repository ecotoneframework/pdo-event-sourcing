<?php

namespace Ecotone\EventSourcing;

use Ecotone\Modelling\Event;

interface EventStreamEmitter
{
    /**
     * @param Event[]|object[]|array[] $streamEvents
     */
    public function linkTo(string $streamName, array $streamEvents): void;
}