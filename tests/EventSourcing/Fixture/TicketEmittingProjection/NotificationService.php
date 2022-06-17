<?php

namespace Test\Ecotone\EventSourcing\Fixture\TicketEmittingProjection;

use Ecotone\EventSourcing\EventStore;
use Ecotone\Messaging\Attribute\Parameter\Reference;
use Ecotone\Modelling\Attribute\EventHandler;
use Ecotone\Modelling\Attribute\QueryHandler;
use Ecotone\Modelling\Event;

final class NotificationService
{
    private array $publishedEvents = [];

    #[QueryHandler("get.notifications")]
    public function getNotifications(#[Reference] EventStore $eventStore): string
    {
        /** @var Event[] $events */
        $events = $eventStore->loadReverse(InProgressTicketList::NAME, count: 1);

        return $events[0]->getPayload()->ticketId;
    }

    #[EventHandler]
    public function subscribeToProjectionEvent(TicketListUpdated $event): void
    {
        $this->publishedEvents[] = $event;
    }

    #[QueryHandler("get.published_events")]
    public function lastPublishedEvent(): array
    {
        return $this->publishedEvents;
    }
}