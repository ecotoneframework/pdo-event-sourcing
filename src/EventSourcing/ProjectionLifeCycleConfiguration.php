<?php


namespace Ecotone\EventSourcing;


class ProjectionLifeCycleConfiguration
{
    private string $projectionName;
    private string $initializationRequestChannel;
    private ?string $resetRequestChannel = null;
    private ?string $deleteRequestChannel = null;

    private function __construct(string $projectionName, string $initializationRequestChannel)
    {
        $this->projectionName = $projectionName;
        $this->initializationRequestChannel = $initializationRequestChannel;
    }

    public static function create(string $projectionName, string $initializationRequestChannel) : static
    {
        return new self($projectionName, $initializationRequestChannel);
    }

    public function withDeleteRequestChannel(string $deleteRequestChannel) : static
    {
        $this->deleteRequestChannel = $deleteRequestChannel;

        return $this;
    }

    public function withResetRequestChannel(string $resetRequestChannel) : static
    {
        $this->resetRequestChannel = $resetRequestChannel;

        return $this;
    }

    public function getProjectionName(): string
    {
        return $this->projectionName;
    }

    public function getInitializationRequestChannel(): string
    {
        return $this->initializationRequestChannel;
    }

    public function getResetRequestChannel(): ?string
    {
        return $this->resetRequestChannel;
    }

    public function getDeleteRequestChannel(): ?string
    {
        return $this->deleteRequestChannel;
    }
}