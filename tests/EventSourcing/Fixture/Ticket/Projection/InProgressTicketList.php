<?php


namespace Test\Ecotone\EventSourcing\Fixture\Ticket\Projection;

use Doctrine\DBAL\Connection;
use Ecotone\EventSourcing\Attribute\Projection;
use Ecotone\EventSourcing\Attribute\ProjectionInitialization;
use Ecotone\Modelling\Attribute\EventHandler;
use Ecotone\Modelling\Attribute\QueryHandler;
use Test\Ecotone\EventSourcing\Fixture\Ticket\Event\TicketWasClosed;
use Test\Ecotone\EventSourcing\Fixture\Ticket\Event\TicketWasRegistered;

#[Projection("allTicketList", "ticket_stream")]
class InProgressTicketList
{
    private Connection $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    #[QueryHandler("getInProgressTickets")]
    public function getTickets() : array
    {
        return $this->connection->executeQuery(<<<SQL
    SELECT * FROM in_progres_tickets
SQL)->fetchAllAssociative();
    }

    #[EventHandler]
    public function addTicket(TicketWasRegistered $event, array $metadata) : void
    {
        $this->connection->executeStatement(<<<SQL
    INSERT INTO in_progress_tickets VALUES (?,?, ?)
SQL, [$event->getTicketId(), $event->getTicketType(), $metadata["timestamp"]]);
    }

    #[EventHandler]
    public function removeTicket(TicketWasClosed $event) : void
    {
        $this->connection->executeStatement(<<<SQL
    DELETE FROM in_progress_tickets WHERE ticket_id = ?
SQL, [$event->getTicketId()]);
    }

    #[ProjectionInitialization()]
    public function initialization() : void
    {
        $this->connection->executeStatement(<<<SQL
    CREATE TABLE IF NOT EXISTS in_progress_tickets (
        ticket_id PRIMARY KEY VARCHAR(36),
        ticket_type VARCHAR(25),
        created_at TIMESTAMP
    )
SQL);
    }
}