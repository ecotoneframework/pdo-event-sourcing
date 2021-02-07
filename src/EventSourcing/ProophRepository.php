<?php

namespace Ecotone\EventSourcing;

use DateTimeImmutable;
use DateTimeZone;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\PDOConnection;
use Ecotone\EventSourcing\StreamConfiguration\SingleStreamConfiguration;
use Ecotone\Messaging\Conversion\ConversionService;
use Ecotone\Messaging\Conversion\MediaType;
use Ecotone\Messaging\Handler\TypeDescriptor;
use Ecotone\Messaging\MessageConverter\HeaderMapper;
use Ecotone\Messaging\MessageHeaders;
use Ecotone\Modelling\EventSourcedRepository;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Metadata\Operator;
use Prooph\EventStore\Stream;
use Ramsey\Uuid\Uuid;

class ProophRepository implements EventSourcedRepository
{
    const STREAM_TABLE = "event_streams";
    const PROJECTIONS_TABLE = "projections";

    const POSTGRES_TABLE_NOT_FOUND_EXCEPTION = "42P01";
    const MYSQL_TABLE_NOT_FOUND_EXCEPTION = 1146;
    const MARIADB_TABLE_NOT_FOUND_EXCEPTION = 1932;
    private EventStore $eventStore;
    private StreamConfiguration $streamConfiguration;
    private HeaderMapper $headerMapper;
    private array $handledAggregateClassNames;
    private EventMapper $eventMapper;
    private ConversionService $conversionService;
    private Connection $connection;
    private string $eventStoreType;
    private string $eventStreamTable;

    public function __construct(string $eventStoreType, string $eventStreamTable, array $handledAggregateClassNames, EventStore $eventStore, StreamConfiguration $streamConfiguration, HeaderMapper $headerMapper, EventMapper $eventMapper, ConversionService $conversionService, Connection $connection)
    {
        $this->eventStoreType = $eventStoreType;
        $this->eventStore = $eventStore;
        $this->streamConfiguration = $streamConfiguration;
        $this->headerMapper = $headerMapper;
        $this->handledAggregateClassNames = $handledAggregateClassNames;
        $this->eventMapper = $eventMapper;
        $this->conversionService = $conversionService;
        $this->connection = $connection;
        $this->eventStreamTable = $eventStreamTable;
    }

    public function canHandle(string $aggregateClassName): bool
    {
        return in_array($aggregateClassName, $this->handledAggregateClassNames);
    }

    public function findBy(string $aggregateClassName, array $identifiers): ?array
    {
        $aggregateId = reset($identifiers);
        $streamName = $this->streamConfiguration->generate($aggregateClassName, $aggregateId);

        if ($this->streamConfiguration->isOneStreamPerAggregate()) {
            try {
                $streamEvents = $this->eventStore->load($streamName, 1);
            } catch (StreamNotFound) { return null; }
        } else {
            $metadataMatcher = new MetadataMatcher();
            $metadataMatcher = $metadataMatcher->withMetadataMatch(
                '_aggregate_type',
                Operator::EQUALS(),
                $aggregateClassName
            );
            $metadataMatcher = $metadataMatcher->withMetadataMatch(
                '_aggregate_id',
                Operator::EQUALS(),
                $aggregateId
            );

            try {
                $streamEvents = $this->eventStore->load($streamName, 1, null, $metadataMatcher);
            } catch (StreamNotFound) { return null; }
        }

        if (!$streamEvents->valid()) {
            return null;
        }

        return $streamEvents;
    }

    public function save(array $identifiers, string $aggregateClassName, array $events, array $metadata, ?int $versionBeforeHandling): void
    {
        $aggregateId = reset($identifiers);
        $streamName = $this->streamConfiguration->generate($aggregateClassName, $aggregateId);

        $proophEvents = [];
        foreach ($events as $eventToConvert) {
            $proophEvents[] = new ProophEvent(
                Uuid::fromString($metadata[MessageHeaders::MESSAGE_ID]),
                new DateTimeImmutable("@" . $metadata[MessageHeaders::TIMESTAMP], new DateTimeZone('UTC')),
                $this->conversionService->convert($eventToConvert, TypeDescriptor::createFromVariable($eventToConvert), MediaType::createApplicationXPHP(), TypeDescriptor::createArrayType(), MediaType::createApplicationXPHP()),
                array_merge(
                    $this->headerMapper->mapFromMessageHeaders($metadata),
                    [
                        '_aggregate_id' => $aggregateId,
                        '_aggregate_type' => $aggregateClassName,
                        '_aggregate_version' => $versionBeforeHandling // @TODO
                    ]
                ),
                $this->eventMapper->mapEventToName($eventToConvert)
            );
        }

//        @TDDO optimize table exists and hasStream
        $sm = $this->connection->getSchemaManager();
        if (!$sm->tablesExist([$this->eventStreamTable])) {
            match ($this->eventStoreType) {
                ProophRepositoryBuilder::EVENT_STORE_TYPE_POSTGRES => $this->createPostgresEventStreamTable(),
                ProophRepositoryBuilder::EVENT_STORE_TYPE_MARIADB => $this->createMariadbEventStreamTable(),
                ProophRepositoryBuilder::EVENT_STORE_TYPE_MYSQL => $this->createMysqlEventStreamTable()
            };
        }
        if (!$sm->tablesExist([self::PROJECTIONS_TABLE])) {
            match ($this->eventStoreType) {
                ProophRepositoryBuilder::EVENT_STORE_TYPE_POSTGRES => $this->createPostgresProjectionTable(),
                ProophRepositoryBuilder::EVENT_STORE_TYPE_MARIADB => $this->createMariadbProjectionTable(),
                ProophRepositoryBuilder::EVENT_STORE_TYPE_MYSQL => $this->createMysqlProjectionTable()
            };
        }

        if (!$this->eventStore->hasStream($streamName)) {
            $this->eventStore->create(new Stream($streamName, new \ArrayIterator($proophEvents), []));
        } else {
            $this->eventStore->appendTo($streamName, new \ArrayIterator($proophEvents));
        }
    }

    private function createMysqlEventStreamTable() : void
    {
        $this->connection->executeStatement(<<<SQL
    CREATE TABLE `event_streams` (
  `no` BIGINT(20) NOT NULL AUTO_INCREMENT,
  `real_stream_name` VARCHAR(150) NOT NULL,
  `stream_name` CHAR(41) NOT NULL,
  `metadata` JSON,
  `category` VARCHAR(150),
  PRIMARY KEY (`no`),
  UNIQUE KEY `ix_rsn` (`real_stream_name`),
  KEY `ix_cat` (`category`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
SQL);
    }

    private function createMariadbEventStreamTable() : void
    {
        $this->connection->executeStatement(<<<SQL
CREATE TABLE `event_streams` (
    `no` BIGINT(20) NOT NULL AUTO_INCREMENT,
    `real_stream_name` VARCHAR(150) NOT NULL,
    `stream_name` CHAR(41) NOT NULL,
    `metadata` LONGTEXT NOT NULL,
    `category` VARCHAR(150),
    CHECK (`metadata` IS NOT NULL OR JSON_VALID(`metadata`)),
    PRIMARY KEY (`no`),
    UNIQUE KEY `ix_rsn` (`real_stream_name`),
    KEY `ix_cat` (`category`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
SQL);
    }

    private function createPostgresEventStreamTable() : void
    {
        $this->connection->executeStatement(<<<SQL
CREATE TABLE event_streams (
  no BIGSERIAL,
  real_stream_name VARCHAR(150) NOT NULL,
  stream_name CHAR(41) NOT NULL,
  metadata JSONB,
  category VARCHAR(150),
  PRIMARY KEY (no),
  UNIQUE (stream_name)
);
CREATE INDEX on event_streams (category);
SQL);
    }

    private function createMysqlProjectionTable(): void
    {
        $this->connection->executeStatement(<<<SQL
CREATE TABLE `projections` (
  `no` BIGINT(20) NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(150) NOT NULL,
  `position` JSON,
  `state` JSON,
  `status` VARCHAR(28) NOT NULL,
  `locked_until` CHAR(26),
  PRIMARY KEY (`no`),
  UNIQUE KEY `ix_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
SQL
        );
    }

    private function createMariadbProjectionTable(): void
    {
        $this->connection->executeStatement(<<<SQL
CREATE TABLE `projections` (
  `no` BIGINT(20) NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(150) NOT NULL,
  `position` LONGTEXT,
  `state` LONGTEXT,
  `status` VARCHAR(28) NOT NULL,
  `locked_until` CHAR(26),
  CHECK (`position` IS NULL OR JSON_VALID(`position`)),
  CHECK (`state` IS NULL OR JSON_VALID(`state`)),
  PRIMARY KEY (`no`),
  UNIQUE KEY `ix_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
SQL
        );
    }

    private function createPostgresProjectionTable(): void
    {
        $this->connection->executeStatement(<<<SQL
CREATE TABLE projections (
  no BIGSERIAL,
  name VARCHAR(150) NOT NULL,
  position JSONB,
  state JSONB,
  status VARCHAR(28) NOT NULL,
  locked_until CHAR(26),
  PRIMARY KEY (no),
  UNIQUE (name)
);
SQL
        );
    }
}