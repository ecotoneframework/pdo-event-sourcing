<?php


namespace Ecotone\EventSourcing;


use Ecotone\Messaging\Handler\Processor\MethodInvoker\Converter\ReferenceBuilder;
use Prooph\EventStore\StreamName;

class ProophRepositoryBuilder
{
    private StreamName $streamName;


    public function canHandle(string $aggregateClassName): bool
    {

    }

    public function build(ReferenceBuilder $referenceBuilder)
    {

    }
}