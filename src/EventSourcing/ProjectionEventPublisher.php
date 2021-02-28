<?php

namespace Ecotone\EventSourcing;

interface ProjectionEventPublisher
{
    public function emit(object $event, array $metadata = []) : void;

    public function linkTo(string $streamName, object $event, array $metadata = []);
}