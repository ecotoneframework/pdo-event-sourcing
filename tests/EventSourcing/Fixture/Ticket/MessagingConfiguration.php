<?php

namespace Test\Ecotone\EventSourcing\Fixture\Ticket;

use Ecotone\Dbal\Configuration\DbalConfiguration;
use Ecotone\Messaging\Attribute\ServiceContext;

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