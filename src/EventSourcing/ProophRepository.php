<?php


namespace Ecotone\EventSourcing;


use Ecotone\Modelling\EventSourcedRepository;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Pdo\Container\AbstractEventStoreFactory;

class ProophRepository implements EventSourcedRepository
{
    private EventStore $eventStore;
    private StreamNameGenerator $streamNameGenerator;


    public function __construct(EventStore $eventStore, StreamNameGenerator $streamNameGenerator)
    {
        $this->eventStore = $eventStore;
        $this->streamNameGenerator = $streamNameGenerator;
    }

    public function canHandle(string $aggregateClassName): bool
    {

    }

    public function findBy(string $aggregateClassName, array $identifiers): ?array
    {

    }

    public function save(array $identifiers, string $aggregateClassName, array $events, array $metadata, ?int $expectedVersion): void
    {
        $streamName = $this->streamNameGenerator->generate($aggregateClassName, reset($identifiers));

    }
}