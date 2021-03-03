<?php


namespace Ecotone\EventSourcing\Config;


use Prooph\EventStore\Projection\ReadModel;

class ProophReadModel implements ReadModel
{
    public function init(): void
    {
        // TODO: Implement init() method.
    }

    public function isInitialized(): bool
    {
        return false;
    }

    public function reset(): void
    {
        // TODO: Implement reset() method.
    }

    public function delete(): void
    {
        // TODO: Implement delete() method.
    }

    public function stack(string $operation, ...$args): void
    {
        return;
    }

    public function persist(): void
    {
        return;
    }
}