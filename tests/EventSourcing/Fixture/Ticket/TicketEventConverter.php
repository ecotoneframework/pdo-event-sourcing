<?php


namespace Test\Ecotone\EventSourcing\Fixture\Ticket;

use Ecotone\Messaging\Attribute\Converter;
use Ecotone\Messaging\Attribute\MediaTypeConverter;
use Test\Ecotone\EventSourcing\Fixture\Ticket\Command\ChangeAssignedPerson;
use Test\Ecotone\EventSourcing\Fixture\Ticket\Event\AssignedPersonWasChanged;
use Test\Ecotone\EventSourcing\Fixture\Ticket\Event\TicketWasRegistered;

class TicketEventConverter
{
    #[Converter]
    public function fromTicketWasRegistered(TicketWasRegistered $event) : array
    {
        return [
            "ticketId" => $event->getTicketId(),
            "ticketType" => $event->getTicketType(),
            "assignedPerson" => $event->getAssignedPerson()
        ];
    }

    #[Converter]
    public function toTicketWasRegistered(array $event) : TicketWasRegistered
    {
        return new TicketWasRegistered($event["ticketId"], $event["assignedPerson"], $event["ticketType"]);
    }

    #[Converter]
    public function fromAssignedPersonWasChanged(AssignedPersonWasChanged $event) : array
    {
        return [
            "ticketId" => $event->getTicketId(),
            "assignedPerson" => $event->getAssignedPerson()
        ];
    }

    #[Converter]
    public function toAssignedPersonWasChanged(array $event) : AssignedPersonWasChanged
    {
        return new AssignedPersonWasChanged($event["ticketId"], $event["assignedPerson"]);
    }
}