<?php

namespace Test\Ecotone\EventSourcing\Fixture\Ticket\Projection;

use Ecotone\EventSourcing\Attribute\Projection;
use Ecotone\Messaging\Attribute\Asynchronous;
use Ecotone\Modelling\Attribute\Aggregate;
use Ecotone\Modelling\Attribute\AggregateIdentifier;
use Ecotone\Modelling\Attribute\EventHandler;
use Test\Ecotone\EventSourcing\Fixture\Ticket\Event\TicketWasRegistered;

#[Asynchronous("someId")]
#[Aggregate]
#[Projection("ticketList", [""])]
class TicketProjection
{
    #[AggregateIdentifier]
    private string $ticketId;
    private string $assignedPerson;
    private string $ticketType;
    private bool $isInProgress;

    private function __construct() {}

    #[EventHandler]
    public static function createFrom(TicketWasRegistered $event) : static
    {
        $self = new static();

        $self->ticketId = $event->getTicketId();
        $self->assignedPerson = $event->getAssignedPerson();
        $self->ticketType = $event->getTicketType();
        $self->isInProgress = true;

        return $self;
    }
}