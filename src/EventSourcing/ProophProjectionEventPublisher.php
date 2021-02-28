<?php


namespace Ecotone\EventSourcing;


use Prooph\EventStore\Projection\Projector;

class ProophProjectionEventPublisher implements ProjectionEventPublisher
{
    /**
     * @var Projector
     */
    private Projector $projector;

    public function __construct(Projector $projector)
    {
        $this->projector = $projector;
    }

    public function emit(object $event, array $metadata = []): void
    {
        // TODO: Implement emit() method.
    }

    public function linkTo(string $streamName, object $event, array $metadata = [])
    {
        // TODO: Implement linkTo() method.
    }
}