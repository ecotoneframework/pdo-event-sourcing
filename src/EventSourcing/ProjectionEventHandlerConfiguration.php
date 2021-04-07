<?php


namespace Ecotone\EventSourcing;


class ProjectionEventHandlerConfiguration
{
    public function __construct(private string $className, private string $methodName, private string $requestChannelName) {}

    public function getClassName(): string
    {
        return $this->className;
    }

    public function getMethodName(): string
    {
        return $this->methodName;
    }

    public function getRequestChannelName(): string
    {
        return $this->requestChannelName;
    }
}