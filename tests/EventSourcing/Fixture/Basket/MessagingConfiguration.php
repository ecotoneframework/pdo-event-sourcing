<?php

namespace Test\Ecotone\EventSourcing\Fixture\Basket;

use Ecotone\Dbal\Configuration\DbalConfiguration;
use Ecotone\EventSourcing\ProjectionRunningConfiguration;
use Ecotone\Messaging\Attribute\ServiceContext;
use Ecotone\Messaging\Endpoint\PollingMetadata;
use Test\Ecotone\EventSourcing\Fixture\Basket\Projection\BasketList;
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

    #[ServiceContext]
    public function setMaximumOneRunForProjections()
    {
        return PollingMetadata::create(BasketList::PROJECTION_NAME)
                    ->setExecutionAmountLimit(3)
                    ->setExecutionTimeLimitInMilliseconds(300);
    }

    #[ServiceContext]
    public function enablePollingProjection()
    {
        return ProjectionRunningConfiguration::createPolling(BasketList::PROJECTION_NAME);
    }
}