<?php


namespace Ecotone\EventSourcing;


use Prooph\EventStore\Projection\ReadModel;
use Prooph\EventStore\Projection\ReadModelProjector;

class ProjectionConfiguration
{
    private string $projectionName;
    private bool $withAllStreams;
    private array $streamNames;
    private array $categories;
    private array $projectionEventHandlers = [];

    private function __construct(string $projectionName, ProjectionLifeCycleConfiguration $projectionLifeCycleConfiguration, bool $withAllStreams, array $streamNames, array $categories)
    {
        $this->projectionName = $projectionName;
        $this->withAllStreams = $withAllStreams;
        $this->streamNames = $streamNames;
        $this->categories = $categories;
    }

    public function fromStream(string $projectionName, ProjectionLifeCycleConfiguration $projectionLifeCycleConfiguration, string $streamName): static
    {
        return new static ($projectionName, $projectionLifeCycleConfiguration,false, [$streamName], []);
    }

    public function fromStreams(string $projectionName, ProjectionLifeCycleConfiguration $projectionLifeCycleConfiguration, string ...$streamNames): static
    {
        return new static($projectionName, $projectionLifeCycleConfiguration,false, $streamNames, []);
    }

    public function fromCategory(string $projectionName, ProjectionLifeCycleConfiguration $projectionLifeCycleConfiguration, string $name): static
    {
        return new static($projectionName, $projectionLifeCycleConfiguration,false, [], [$name]);
    }

    public function fromCategories(string $projectionName, ProjectionLifeCycleConfiguration $projectionLifeCycleConfiguration, string ...$names): static
    {
        return new static($projectionName, $projectionLifeCycleConfiguration,false, [], $names);
    }

    public function fromAll(string $projectionName, ProjectionLifeCycleConfiguration $projectionLifeCycleConfiguration): static
    {
        return new static($projectionName, $projectionLifeCycleConfiguration,true, [], []);
    }

    public function withProjectionEventHandler(string $eventName, string $eventHandlerRequestChannel) : static
    {
        $this->projectionEventHandlers[$eventName][] = $eventHandlerRequestChannel;

        return $this;
    }

    public function getProjectionName(): string
    {
        return $this->projectionName;
    }

    public function isWithAllStreams(): bool
    {
        return $this->withAllStreams;
    }

    public function getStreamNames(): array
    {
        return $this->streamNames;
    }

    public function getCategories(): array
    {
        return $this->categories;
    }
}