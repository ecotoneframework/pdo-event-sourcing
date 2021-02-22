<?php

namespace Ecotone\EventSourcing;

use Prooph\EventStore\Pdo\Projection\MariaDbProjectionManager;
use Prooph\EventStore\Pdo\Projection\PostgresProjectionManager;
use Prooph\EventStore\Projection\ProjectionManager;
use Prooph\EventStore\Projection\ProjectionStatus;
use Prooph\EventStore\Projection\Projector;
use Prooph\EventStore\Projection\Query;
use Prooph\EventStore\Projection\ReadModel;
use Prooph\EventStore\Projection\ReadModelProjector;

class LazyProophProjectionManager implements ProjectionManager
{
    private EventStoreProophIntegration $eventStore;
    private ?ProjectionManager $lazyInitializedProjectionManager = null;

    public function __construct(EventStoreProophIntegration $eventStore)
    {
        $this->eventStore = $eventStore;
    }

    private function getProjectionManager() : ProjectionManager
    {
        if ($this->lazyInitializedProjectionManager) {
            return $this->lazyInitializedProjectionManager;
        }

        $eventStoreType = $this->eventStore->getWrappedEventStore()->getEventStoreType();

        $this->lazyInitializedProjectionManager = match ($eventStoreType) {
            LazyProophEventStore::EVENT_STORE_TYPE_POSTGRES => new PostgresProjectionManager($this->eventStore->getWrappedProophEventStore(), $this->eventStore->getWrappedConnection(), $this->eventStore->getEventStreamTable(), $this->eventStore->getProjectionsTable()),
            LazyProophEventStore::EVENT_STORE_TYPE_MYSQL => new PostgresProjectionManager($this->eventStore->getWrappedProophEventStore(), $this->eventStore->getWrappedConnection(), $this->eventStore->getEventStreamTable(), $this->eventStore->getProjectionsTable()),
            LazyProophEventStore::EVENT_STORE_TYPE_MARIADB => new MariaDbProjectionManager($this->eventStore->getWrappedProophEventStore(), $this->eventStore->getWrappedConnection(), $this->eventStore->getEventStreamTable(), $this->eventStore->getProjectionsTable())
        };

        return $this->lazyInitializedProjectionManager;
    }

    public function createQuery(): Query
    {
        return $this->getProjectionManager()->createQuery();
    }

    public function createProjection(string $name, array $options = []): Projector
    {
        return $this->getProjectionManager()->createProjection($name, $options);
    }

    public function createReadModelProjection(string $name, ReadModel $readModel, array $options = []): ReadModelProjector
    {
        return $this->getProjectionManager()->createReadModelProjection($name, $readModel, $options);
    }

    public function deleteProjection(string $name, bool $deleteEmittedEvents): void
    {
        $this->getProjectionManager()->deleteProjection($name, $deleteEmittedEvents);
    }

    public function resetProjection(string $name): void
    {
        $this->getProjectionManager()->resetProjection($name);
    }

    public function stopProjection(string $name): void
    {
        $this->getProjectionManager()->stopProjection($name);
    }

    public function fetchProjectionNames(?string $filter, int $limit = 20, int $offset = 0): array
    {
        $this->getProjectionManager()->fetchProjectionNames($filter, $limit, $offset);
    }

    public function fetchProjectionNamesRegex(string $regex, int $limit = 20, int $offset = 0): array
    {
        return $this->getProjectionManager()->fetchProjectionNamesRegex($regex, $limit, $offset);
    }

    public function fetchProjectionStatus(string $name): ProjectionStatus
    {
        return $this->getProjectionManager()->fetchProjectionStatus($name);
    }

    public function fetchProjectionStreamPositions(string $name): array
    {
        return $this->getProjectionManager()->fetchProjectionStreamPositions($name);
    }

    public function fetchProjectionState(string $name): array
    {
        return $this->getProjectionManager()->fetchProjectionState($name);
    }
}