<?php

namespace Ecotone\EventSourcing;

use Ecotone\Messaging\Support\Assert;

class ProjectionRunningConfiguration
{
    private const EVENT_DRIVEN = "synchronous";
    private const POLLING      = "continuesPolling";

    private function __construct(private string $projectionName, private string $runningType, private bool $initializeOnStartup) {}

    public static function createEventDriven(string $projectionName, bool $initializeOnStartup = true): static
    {
        return new self($projectionName, self::EVENT_DRIVEN, $initializeOnStartup);
    }

    public static function createPolling(string $projectionName, bool $initializeOnStartup = true) : static
    {
        return new self($projectionName, self::POLLING, $initializeOnStartup);
    }

    public function getProjectionName(): string
    {
        return $this->projectionName;
    }

    public function isPolling(): bool
    {
        return $this->runningType === self::POLLING;
    }

    public function isEventDriven(): bool
    {
        return $this->runningType === self::EVENT_DRIVEN;
    }

    public function isInitializedOnStartup(): bool
    {
        return $this->initializeOnStartup;
    }
}