<?php

namespace Test\Ecotone\EventSourcing\Fixture\Ticket;

use Ecotone\Dbal\Configuration\DbalConfiguration;
use Ecotone\EventSourcing\ProjectionRunningConfiguration;
use Ecotone\Messaging\Attribute\ServiceContext;
use Ecotone\Messaging\Endpoint\PollingMetadata;
use Test\Ecotone\EventSourcing\Fixture\Ticket\Projection\InProgressTicketList;

class MessagingConfiguration
{
    #[ServiceContext]
    public function turnOffTransactions()
    {
        return DbalConfiguration::createWithDefaults()
                ->withTransactionOnCommandBus(false)
                ->withTransactionOnAsynchronousEndpoints(false)
                ->withTransactionOnConsoleCommands(false);
    }
}