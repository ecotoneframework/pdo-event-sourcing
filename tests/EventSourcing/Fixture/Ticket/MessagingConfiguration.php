<?php


namespace Test\Ecotone\EventSourcing\Fixture\Ticket;


use Ecotone\EventSourcing\ProophRepositoryBuilder;
use Ecotone\Messaging\Attribute\ServiceContext;
use Ecotone\Modelling\RepositoryBuilder;

class MessagingConfiguration
{
    #[ServiceContext]
    public function repository() : RepositoryBuilder
    {
        return ProophRepositoryBuilder::create(ProophRepositoryBuilder::EVENT_STORE_TYPE_POSTGRES);
    }
}