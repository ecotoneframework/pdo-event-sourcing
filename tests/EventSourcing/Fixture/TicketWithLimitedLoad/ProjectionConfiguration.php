<?php

namespace Test\Ecotone\EventSourcing\Fixture\TicketWithLimitedLoad;

use Ecotone\Dbal\DbalBackedMessageChannelBuilder;
use Ecotone\EventSourcing\EventSourcingConfiguration;
use Ecotone\EventSourcing\ProjectionRunningConfiguration;
use Ecotone\Messaging\Attribute\ServiceContext;
use Ecotone\Messaging\Channel\SimpleMessageChannelBuilder;
use Ecotone\Messaging\Endpoint\PollingMetadata;
use Test\Ecotone\EventSourcing\Fixture\Ticket\Projection\InProgressTicketList;

class ProjectionConfiguration
{
    #[ServiceContext]
    public function configureProjection()
    {
        return [
//            EventSourcingConfiguration::createWithDefaults()
//                ->withLoadBatchSize(1)
        ];
    }
}