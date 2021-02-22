<?php


namespace Test\Ecotone\EventSourcing\Integration;


use Ecotone\EventSourcing\LazyEventStore;
use Prooph\EventStore\Pdo\Projection\PostgresProjectionManager;
use Prooph\EventStore\StreamName;
use Test\Ecotone\EventSourcing\EventSourcingMessagingTest;
use Test\Ecotone\EventSourcing\Fixture\Ticket\Event\TicketWasRegistered;

class LowLevelProjectionTest extends EventSourcingMessagingTest
{
    public function test_building_projection()
    {
        $eventStore = LazyEventStore::startWithDefaults($this->getConnectionFactory());
        $projectionManager = new PostgresProjectionManager($eventStore->getEventStore(), $eventStore->getWrappedConnection());

        $eventStore->appendTo(new StreamName("tickets"), new \ArrayIterator([
            new TicketWasRegistered(1, "johny", "information")
        ]));

        $projectionManager
            ->createProjection("tickets")
            ->fromStream("ticket")
            ->when([
                TicketWasRegistered::class => function($state, TicketWasRegistered $event) {
                    die("test");
                }
            ])
            ->run(false);
    }
}