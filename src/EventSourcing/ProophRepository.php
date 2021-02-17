<?php

namespace Ecotone\EventSourcing;

use DateTimeImmutable;
use DateTimeZone;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\PDOConnection;
use Ecotone\EventSourcing\StreamConfiguration\SingleStreamConfiguration;
use Ecotone\Messaging\Conversion\ConversionService;
use Ecotone\Messaging\Conversion\MediaType;
use Ecotone\Messaging\Handler\ClassDefinition;
use Ecotone\Messaging\Handler\TypeDescriptor;
use Ecotone\Messaging\MessageConverter\HeaderMapper;
use Ecotone\Messaging\MessageHeaders;
use Ecotone\Modelling\EventSourcedRepository;
use Ecotone\Modelling\EventStream;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Metadata\Operator;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use Ramsey\Uuid\Uuid;

class ProophRepository implements EventSourcedRepository
{
    const STREAM_TABLE = "event_streams";
    const PROJECTIONS_TABLE = "projections";

    const POSTGRES_TABLE_NOT_FOUND_EXCEPTION = "42P01";
    const MYSQL_TABLE_NOT_FOUND_EXCEPTION = 1146;
    const MARIADB_TABLE_NOT_FOUND_EXCEPTION = 1932;
    const AGGREGATE_VERSION = '_aggregate_version';
    const AGGREGATE_TYPE = '_aggregate_type';
    const AGGREGATE_ID = '_aggregate_id';
    private LazyEventStore $eventStore;
    private HeaderMapper $headerMapper;
    private array $handledAggregateClassNames;
    private EventMapper $eventMapper;
    private ConversionService $conversionService;
    private string $eventStreamTable;
    private array $aggregateClassToStreamName;

    public function __construct(string $eventStreamTable, array $handledAggregateClassNames, LazyEventStore $eventStore, HeaderMapper $headerMapper, EventMapper $eventMapper, ConversionService $conversionService, array $aggregateClassStreamNames)
    {
        $this->eventStore = $eventStore;
        $this->headerMapper = $headerMapper;
        $this->handledAggregateClassNames = $handledAggregateClassNames;
        $this->eventMapper = $eventMapper;
        $this->conversionService = $conversionService;
        $this->eventStreamTable = $eventStreamTable;
        $this->aggregateClassToStreamName = $aggregateClassStreamNames;
    }

    public function canHandle(string $aggregateClassName): bool
    {
        return in_array($aggregateClassName, $this->handledAggregateClassNames);
    }

    public function findBy(string $aggregateClassName, array $identifiers): EventStream
    {
        $aggregateId = reset($identifiers);
        $streamName = $this->getStreamName($aggregateClassName, $aggregateId);

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            self::AGGREGATE_TYPE,
            Operator::EQUALS(),
            $aggregateClassName
        );
        $metadataMatcher = $metadataMatcher->withMetadataMatch(
            self::AGGREGATE_ID,
            Operator::EQUALS(),
            $aggregateId
        );

        try {
            $streamEvents = $this->eventStore->load($streamName, 1, null, $metadataMatcher);
        } catch (StreamNotFound) { return EventStream::createEmpty(); }

        if (!$streamEvents->valid()) {
            return EventStream::createEmpty();
        }

        $events = [];
        $aggregateVersion = 0;
        $sourcePHPType = TypeDescriptor::createArrayType();
        $PHPMediaType = MediaType::createApplicationXPHP();
        /** @var ProophEvent $event */
        while ($event = $streamEvents->current()) {
            $aggregateVersion = $event->metadata()[self::AGGREGATE_VERSION];
            $events[] = $this->conversionService->convert($event->payload(), $sourcePHPType, $PHPMediaType, TypeDescriptor::create($event->messageName()), $PHPMediaType);

            $streamEvents->next();
        }

        return EventStream::createWith($aggregateVersion, $events);
    }

    public function save(array $identifiers, string $aggregateClassName, array $events, array $metadata, int $versionBeforeHandling): void
    {
        $aggregateId = reset($identifiers);
        $streamName = $this->getStreamName($aggregateClassName, $aggregateId);

        $proophEvents = [];
        $numberOfEvents = count($events);
        for ($eventNumber = 0; $eventNumber < $numberOfEvents; $eventNumber++) {
            $eventToConvert = $events[$eventNumber];
            $proophEvents[] = new ProophEvent(
                Uuid::fromString($metadata[MessageHeaders::MESSAGE_ID]),
                new DateTimeImmutable("@" . $metadata[MessageHeaders::TIMESTAMP], new DateTimeZone('UTC')),
                $this->conversionService->convert($eventToConvert, TypeDescriptor::createFromVariable($eventToConvert), MediaType::createApplicationXPHP(), TypeDescriptor::createArrayType(), MediaType::createApplicationXPHP()),
                array_merge(
                    $this->headerMapper->mapFromMessageHeaders($metadata),
                    [
                        self::AGGREGATE_ID => $aggregateId,
                        self::AGGREGATE_TYPE => $aggregateClassName,
                        self::AGGREGATE_VERSION => $versionBeforeHandling + ($eventNumber + 1)
                    ]
                ),
                $this->eventMapper->mapEventToName($eventToConvert)
            );
        }

//        @TDDO optimize table exists and hasStream
        $sm = $this->eventStore->getConnection()->getSchemaManager();
        if (!$sm->tablesExist([$this->eventStreamTable])) {
            match ($this->eventStore->getEventStoreType()) {
                ProophRepositoryBuilder::EVENT_STORE_TYPE_POSTGRES => $this->createPostgresEventStreamTable(),
                ProophRepositoryBuilder::EVENT_STORE_TYPE_MARIADB => $this->createMariadbEventStreamTable(),
                ProophRepositoryBuilder::EVENT_STORE_TYPE_MYSQL => $this->createMysqlEventStreamTable()
            };
        }
        if (!$sm->tablesExist([self::PROJECTIONS_TABLE])) {
            match ($this->eventStore->getEventStoreType()) {
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
        $this->eventStore->getConnection()->executeStatement(<<<SQL
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
        $this->eventStore->getConnection()->executeStatement(<<<SQL
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
        $this->eventStore->getConnection()->executeStatement(<<<SQL
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
        $this->eventStore->getConnection()->executeStatement(<<<SQL
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
        $this->eventStore->getConnection()->executeStatement(<<<SQL
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
        $this->eventStore->getConnection()->executeStatement(<<<SQL
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

    private function getStreamName(string $aggregateClassName, mixed $aggregateId): StreamName
    {
        $prefix = $aggregateClassName;
        if (array_key_exists($aggregateClassName, $this->aggregateClassToStreamName)) {
            $prefix =  $this->aggregateClassToStreamName[$aggregateClassName];
        }

        return new StreamName($prefix . "-" . $aggregateId);
    }
}