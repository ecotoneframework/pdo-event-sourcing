<?php


namespace Ecotone\EventSourcing\Attribute;

#[\Attribute(\Attribute::TARGET_CLASS)]
class StreamName
{
    private string $name;

    public function __construct(string $name)
    {
        $this->name = $name;
    }

    public function getName(): string
    {
        return $this->name;
    }
}