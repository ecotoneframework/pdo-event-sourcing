<?php


namespace Test\Ecotone\EventSourcing\Fixture\Ticket\Projection;

#[ProjectionApi("ticketList")]
class TicketProjectionApi
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