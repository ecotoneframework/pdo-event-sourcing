<?php


namespace Test\Ecotone\EventSourcing\Fixture\Ticket\Projection;

class TicketReadModel
{
    #[ProjectionInitialization]
    public function initialization() : void
    {

    }

    #[ProjectionIsInitialized]
    public function isInitialized() : bool
    {

    }

    #[ProjectionReset]
    public function reset() : void
    {

    }

    #[ProjectionDelete]
    public function delete() : void
    {

    }
}