<?php

namespace Test\Ecotone\EventSourcing\Fixture\Ticket;

use Ecotone\EventSourcing\Attribute\StreamName;
use Ecotone\Modelling\Attribute\Aggregate;
use Ecotone\Modelling\Attribute\AggregateFactory;
use Ecotone\Modelling\Attribute\AggregateIdentifier;
use Ecotone\Modelling\Attribute\CommandHandler;
use Test\Ecotone\EventSourcing\Fixture\Ticket\Command\ChangeAssignedPerson;
use Test\Ecotone\EventSourcing\Fixture\Ticket\Command\RegisterTicket;
use Test\Ecotone\EventSourcing\Fixture\Ticket\Event\AssignedPersonWasChanged;
use Test\Ecotone\EventSourcing\Fixture\Ticket\Event\TicketWasRegistered;
use Test\Ecotone\Modelling\Fixture\InterceptedCommandAggregate\EventWasLogged;

#[Aggregate]
#[StreamName("ticket_stream")]
class Ticket
{
    #[AggregateIdentifier]
    private string $ticketId;
    private string $assignedPerson;
    private string $ticketType;

    private function __construct() {}

    #[CommandHandler]
    public static function register(RegisterTicket $command) : array
    {
        return [new TicketWasRegistered($command->getTicketId(), $command->getAssignedPerson(), $command->getTicketType())];
    }

    #[CommandHandler]
    public function changeAssignedPerson(ChangeAssignedPerson $command) : array
    {
        return [new AssignedPersonWasChanged($command->getTicketId(), $command->getAssignedPerson())];
    }

    #[AggregateFactory]
    public static function restoreFrom(array $events) : self
    {
        $ticket = new Ticket();

        foreach ($events as $event) {
            match (get_class($event)) {
                TicketWasRegistered::class => $ticket->applyTicketWasRegistered($event),
                AssignedPersonWasChanged::class => $ticket->applyAssignedPersonWasChanged($event)
            };
        }

        return $ticket;
    }

    private function applyTicketWasRegistered(TicketWasRegistered $event) : void
    {
        $this->ticketId       = $event->getTicketId();
        $this->assignedPerson = $event->getAssignedPerson();
        $this->ticketType     = $event->getTicketType();
    }

    private function applyAssignedPersonWasChanged(AssignedPersonWasChanged $event) : void
    {
        $this->assignedPerson = $event->getAssignedPerson();
    }
}