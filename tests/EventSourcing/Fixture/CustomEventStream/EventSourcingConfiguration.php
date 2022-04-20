<?php

namespace Test\Ecotone\EventSourcing\Fixture\CustomEventStream;

use Ecotone\EventSourcing\FromProophMessageToArrayConverter;
use Ecotone\Messaging\Attribute\ServiceContext;
use Prooph\EventStore\Pdo\PersistenceStrategy\PostgresSingleStreamStrategy;

class EventSourcingConfiguration
{
    #[ServiceContext]
    public function aggregateStreamStrategy()
    {
        return \Ecotone\EventSourcing\EventSourcingConfiguration::createWithDefaults()
            ->withCustomPersistenceStrategy(new PostgresSingleStreamStrategy(new FromProophMessageToArrayConverter()));
    }
}
