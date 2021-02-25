<?php

namespace Test\Ecotone\EventSourcing\Behat\Bootstrap;

use Behat\Gherkin\Node\TableNode;
use Behat\Behat\Tester\Exception\PendingException;
use Behat\Behat\Context\Context;
use Doctrine\DBAL\Connection;
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
use Test\Ecotone\EventSourcing\Fixture\Ticket\Command\RegisterTicket;
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
        switch ($namespace) {
            case "Test\Ecotone\EventSourcing\Fixture\Ticket":
            {
                $objects = [new TicketEventConverter()];
                break;
            }
            default:
            {
                throw new InvalidArgumentException("Namespace {$namespace} not yet implemented");
            }
        }

        $managerRegistryConnectionFactory = new DbalConnectionFactory(["dsn" => 'pgsql://ecotone:secret@database:5432/ecotone']);
        self::$connection = $managerRegistryConnectionFactory->createContext()->getDbalConnection();

        self::$messagingSystem = EcotoneLiteConfiguration::createWithConfiguration(
            __DIR__ . "/../../../../",
            InMemoryPSRContainer::createFromObjects(array_merge($objects, ["managerRegistry" => $managerRegistryConnectionFactory, DbalConnectionFactory::class => $managerRegistryConnectionFactory])),
            ServiceConfiguration::createWithDefaults()
                ->withNamespaces([$namespace])
                ->withCacheDirectoryPath(sys_get_temp_dir() . DIRECTORY_SEPARATOR . Uuid::uuid4()->toString()),
            []
        );

        self::$connection->beginTransaction();
    }

    protected function tearDown(): void
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
            $ticketType,
            $assignedPerson
        ));
    }

    /**
     * @Then ticket I should see tickets:
     */
    public function ticketIShouldSeeTickets(TableNode $table)
    {
        throw new PendingException();
    }
}
