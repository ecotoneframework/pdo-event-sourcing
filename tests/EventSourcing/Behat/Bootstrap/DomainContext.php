<?php

namespace Test\Ecotone\EventSourcing\Behat\Bootstrap;

use Behat\Gherkin\Node\TableNode;
use Behat\Behat\Tester\Exception\PendingException;
use Behat\Behat\Context\Context;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception\TableNotFoundException;
use Ecotone\EventSourcing\Config\EventSourcingModule;
use Ecotone\Lite\EcotoneLiteConfiguration;
use Ecotone\Lite\InMemoryPSRContainer;
use Ecotone\Messaging\Config\ConfiguredMessagingSystem;
use Ecotone\Messaging\Config\ServiceConfiguration;
use Ecotone\Modelling\CommandBus;
use Ecotone\Modelling\QueryBus;
use Enqueue\Dbal\DbalConnectionFactory;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;
use Ramsey\Uuid\Uuid;
use Test\Ecotone\EventSourcing\Fixture\Ticket\Command\CloseTicket;
use Test\Ecotone\EventSourcing\Fixture\Ticket\Command\RegisterTicket;
use Test\Ecotone\EventSourcing\Fixture\Ticket\Projection\InProgressTicketList;
use Test\Ecotone\EventSourcing\Fixture\Ticket\TicketEventConverter;

class DomainContext extends TestCase implements Context
{
    private static ConfiguredMessagingSystem $messagingSystem;
    private static Connection $connection;

    /**
     * @Given I active messaging for namespace :namespace
     */
    public function iActiveMessagingForNamespace(string $namespace)
    {
        $managerRegistryConnectionFactory = new DbalConnectionFactory(["dsn" => 'pgsql://ecotone:secret@database:5432/ecotone']);
        self::$connection = $managerRegistryConnectionFactory->createContext()->getDbalConnection();

        switch ($namespace) {
            case "Test\Ecotone\EventSourcing\Fixture\Ticket":
            {
                $objects = [new TicketEventConverter(), new InProgressTicketList(self::$connection)];
                break;
            }
            default:
            {
                throw new InvalidArgumentException("Namespace {$namespace} not yet implemented");
            }
        }

        self::$messagingSystem = EcotoneLiteConfiguration::createWithConfiguration(
            __DIR__ . "/../../../../",
            InMemoryPSRContainer::createFromObjects(array_merge(
                $objects,
                [
                    "managerRegistry" => $managerRegistryConnectionFactory,
                    DbalConnectionFactory::class => $managerRegistryConnectionFactory
                ])
            ),
            ServiceConfiguration::createWithDefaults()
                ->withNamespaces([$namespace])
                ->withCacheDirectoryPath(sys_get_temp_dir() . DIRECTORY_SEPARATOR . Uuid::uuid4()->toString()),
            []
        );

        self::$connection->beginTransaction();
    }

    /**
     * @AfterScenario
     */
    public function rollBack(): void
    {
        self::$connection->rollBack();
    }

    private function getCommandBus(): CommandBus
    {
        return self::$messagingSystem->getGatewayByName(CommandBus::class);
    }

    private function getQueryBus(): QueryBus
    {
        return self::$messagingSystem->getGatewayByName(QueryBus::class);
    }

    /**
     * @When I register :ticketType ticket :id with assignation to :assignedPerson
     */
    public function iRegisterTicketWithAssignationTo(string $ticketType, int $id, string $assignedPerson)
    {
        $this->getCommandBus()->send(new RegisterTicket(
            $id,
            $assignedPerson,
            $ticketType
        ));
        self::$messagingSystem->runAsynchronouslyRunningEndpoint(InProgressTicketList::IN_PROGRESS_TICKET_PROJECTION);
    }

    /**
     * @Then I should see tickets in progress:
     */
    public function iShouldSeeTicketsInProgress(TableNode $table)
    {
        $this->assertEquals(
            $table->getHash(),
            $this->getQueryBus()->sendWithRouting("getInProgressTickets", [])
        );
    }

    /**
     * @When I close ticket with id :arg1
     */
    public function iCloseTicketWithId(string $ticketId)
    {
        $this->getCommandBus()->send(new CloseTicket($ticketId));
        self::$messagingSystem->runAsynchronouslyRunningEndpoint(InProgressTicketList::IN_PROGRESS_TICKET_PROJECTION);
    }

    /**
     * @When I delete projection for all in progress tickets
     */
    public function iDeleteProjectionForAllInProgressTickets()
    {
        self::$messagingSystem->runConsoleCommand(EventSourcingModule::ECOTONE_ES_DELETE_PROJECTION, ["name" => InProgressTicketList::IN_PROGRESS_TICKET_PROJECTION]);
        self::$messagingSystem->runAsynchronouslyRunningEndpoint(InProgressTicketList::IN_PROGRESS_TICKET_PROJECTION);
    }

    /**
     * @Then there should be no in progress ticket list
     */
    public function thereShouldBeNoInProgressTicketList()
    {
        $wasProjectionDeleted = false;
        try {
            $this->getQueryBus()->sendWithRouting("getInProgressTickets", []);
        }catch (TableNotFoundException $exception) {
            $wasProjectionDeleted = true;
        }

        $this->assertTrue($wasProjectionDeleted, "Projection was not deleted");
    }

    /**
     * @When I reset the projection for in progress tickets
     */
    public function iResetTheProjectionForInProgressTickets()
    {
        self::$messagingSystem->runConsoleCommand(EventSourcingModule::ECOTONE_ES_RESET_PROJECTION, ["name" => InProgressTicketList::IN_PROGRESS_TICKET_PROJECTION]);
        self::$messagingSystem->runAsynchronouslyRunningEndpoint(InProgressTicketList::IN_PROGRESS_TICKET_PROJECTION);
    }

    /**
     * @When I stop the projection for in progress tickets
     */
    public function iStopTheProjectionForInProgressTickets()
    {
        self::$messagingSystem->runConsoleCommand(EventSourcingModule::ECOTONE_ES_STOP_PROJECTION, ["name" => InProgressTicketList::IN_PROGRESS_TICKET_PROJECTION]);
    }
}
