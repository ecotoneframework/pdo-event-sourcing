<?php

namespace Test\Ecotone\Dbal\Behat\Bootstrap;

use Behat\Behat\Tester\Exception\PendingException;
use Behat\Behat\Context\Context;
use Doctrine\Common\Annotations\AnnotationException;
use Ecotone\Dbal\Recoverability\DbalDeadLetter;
use Ecotone\Dbal\Recoverability\DeadLetterGateway;
use Ecotone\Lite\EcotoneLiteConfiguration;
use Ecotone\Lite\InMemoryPSRContainer;
use Ecotone\Messaging\Config\ServiceConfiguration;
use Ecotone\Messaging\Config\ConfigurationException;
use Ecotone\Messaging\Config\ConfiguredMessagingSystem;
use Ecotone\Messaging\Conversion\MediaType;
use Ecotone\Messaging\MessagingException;
use Ecotone\Messaging\Support\InvalidArgumentException;
use Ecotone\Modelling\CommandBus;
use Ecotone\Modelling\QueryBus;
use Enqueue\Dbal\DbalConnectionFactory;
use Enqueue\Dbal\ManagerRegistryConnectionFactory;
use PHPUnit\Framework\TestCase;
use Ramsey\Uuid\Uuid;
use ReflectionException;
use Test\Ecotone\Dbal\DbalConnectionManagerRegistryWrapper;
use Test\Ecotone\Dbal\Fixture\DeadLetter\OrderGateway;
use Test\Ecotone\Dbal\Fixture\Transaction\OrderService;
use Test\Ecotone\Modelling\Fixture\OrderAggregate\OrderErrorHandler;

/**
 * Defines application features from the specific context.
 */
class DomainContext extends TestCase implements Context
{
    /**
     * @var ConfiguredMessagingSystem
     */
    private static $messagingSystem;

    /**
     * @Given I active messaging for namespace :namespace
     */
    public function iActiveMessagingForNamespace(string $namespace)
    {
        switch ($namespace) {
            case "Test\Ecotone\EventSourcing\Fixture\Ticket": {
                $objects = [
                    new OrderService()
                ];
                break;
            }
            default: {
                throw new \InvalidArgumentException("Namespace {$namespace} not yet implemented");
            }
        }

        $managerRegistryConnectionFactory = new ManagerRegistryConnectionFactory(new DbalConnectionManagerRegistryWrapper(new DbalConnectionFactory(["dsn" => 'pgsql://ecotone:secret@database:5432/ecotone'])));
        $connection = $managerRegistryConnectionFactory->createContext()->getDbalConnection();
        $isTableExists = $connection->executeQuery(
            <<<SQL
SELECT EXISTS (
   SELECT FROM information_schema.tables 
   WHERE  table_name   = 'enqueue'
   );
SQL
        )->fetchOne();

        self::$messagingSystem            = EcotoneLiteConfiguration::createWithConfiguration(
            __DIR__ . "/../../../../",
            InMemoryPSRContainer::createFromObjects(array_merge($objects, ["managerRegistry" => $managerRegistryConnectionFactory, DbalConnectionFactory::class => $managerRegistryConnectionFactory])),
            ServiceConfiguration::createWithDefaults()
                ->withNamespaces([$namespace])
                ->withCacheDirectoryPath(sys_get_temp_dir() . DIRECTORY_SEPARATOR . Uuid::uuid4()->toString()),
            []
        );
    }

    private function deleteFromTableExists(string $tableName, \Doctrine\DBAL\Connection $connection) : void
    {
        $doesExists = $connection->executeQuery(
            <<<SQL
SELECT EXISTS (
   SELECT FROM information_schema.tables 
   WHERE  table_name   = :tableName
   );
SQL, ["tableName" => $tableName]
        )->fetchOne();

        if ($doesExists) {
            $connection->executeUpdate("DELETE FROM " . $tableName);
        }
    }

    private function getCommandBus(): CommandBus
    {
        return self::$messagingSystem->getGatewayByName(CommandBus::class);
    }

    private function getQueryBus() : QueryBus
    {
        return self::$messagingSystem->getGatewayByName(QueryBus::class);
    }
}
