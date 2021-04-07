<?php


namespace Ecotone\EventSourcing;


use Ecotone\Messaging\Support\Assert;
use Prooph\EventStore\Pdo\Projection\GapDetection;
use Prooph\EventStore\Pdo\Projection\PdoEventStoreReadModelProjector;
use Prooph\EventStore\Projection\ReadModel;
use Prooph\EventStore\Projection\ReadModelProjector;

class ProjectionSetupConfiguration
{
    private string $projectionName;
    private ProjectionLifeCycleConfiguration $projectionLifeCycleConfiguration;
    private bool $withAllStreams;
    private array $streamNames;
    private array $categories;
    /** @var ProjectionEventHandlerConfiguration[] */
    private array $projectionEventHandlers = [];
    /** @var array http://docs.getprooph.org/event-store/projections.html#Options https://github.com/prooph/pdo-event-store/pull/221/files */
    private array $projectionOptions;
    private bool $keepStateBetweenEvents = true;

    private function __construct(string $projectionName, ProjectionLifeCycleConfiguration $projectionLifeCycleConfiguration, bool $withAllStreams, array $streamNames, array $categories)
    {
        $this->projectionName = $projectionName;
        $this->withAllStreams = $withAllStreams;
        $this->streamNames = $streamNames;
        $this->categories = $categories;
        $this->projectionLifeCycleConfiguration = $projectionLifeCycleConfiguration;

        $this->projectionOptions = [
            PdoEventStoreReadModelProjector::OPTION_GAP_DETECTION => new GapDetection(),
//            PdoEventStoreReadModelProjector::DEFAULT_LOCK_TIMEOUT_MS => 0,
//            PdoEventStoreReadModelProjector::OPTION_UPDATE_LOCK_THRESHOLD => 0
        ];
    }

    public static function fromStream(string $projectionName, ProjectionLifeCycleConfiguration $projectionLifeCycleConfiguration, string $streamName): static
    {
        return new static ($projectionName, $projectionLifeCycleConfiguration,false, [$streamName], []);
    }

    public static function fromStreams(string $projectionName, ProjectionLifeCycleConfiguration $projectionLifeCycleConfiguration, string ...$streamNames): static
    {
        return new static($projectionName, $projectionLifeCycleConfiguration,false, $streamNames, []);
    }

    public static function fromCategory(string $projectionName, ProjectionLifeCycleConfiguration $projectionLifeCycleConfiguration, string $name): static
    {
        return new static($projectionName, $projectionLifeCycleConfiguration,false, [], [$name]);
    }

    public static function fromCategories(string $projectionName, ProjectionLifeCycleConfiguration $projectionLifeCycleConfiguration, string ...$names): static
    {
        return new static($projectionName, $projectionLifeCycleConfiguration,false, [], $names);
    }

    public static function fromAll(string $projectionName, ProjectionLifeCycleConfiguration $projectionLifeCycleConfiguration): static
    {
        return new static($projectionName, $projectionLifeCycleConfiguration,true, [], []);
    }

    public function withKeepingStateBetweenEvents(bool $keepState) : static
    {
        $this->keepStateBetweenEvents = $keepState;

        return $this;
    }

    public function isKeepingStateBetweenEvents(): bool
    {
        return $this->keepStateBetweenEvents;
    }

    public function withProjectionEventHandler(string $eventName, string $className, string $methodName, string $eventHandlerRequestChannel) : static
    {
        Assert::keyNotExists($this->projectionEventHandlers, $eventName, "Projection {$this->projectionName} has incorrect configuration. Can't register event handler twice for the same event {$eventName}");

        $this->projectionEventHandlers[$eventName] = new ProjectionEventHandlerConfiguration($className, $methodName, $eventHandlerRequestChannel);

        return $this;
    }

    public function withOptions(array $options) : static
    {
        $this->projectionOptions = $options;

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

    public function getProjectionLifeCycleConfiguration(): ProjectionLifeCycleConfiguration
    {
        return $this->projectionLifeCycleConfiguration;
    }

    public function getProjectionEventHandlers(): array
    {
        return $this->projectionEventHandlers;
    }

    public function getProjectionOptions(): array
    {
        return $this->projectionOptions;
    }
}