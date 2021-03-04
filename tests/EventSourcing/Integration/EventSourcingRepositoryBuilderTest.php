<?php


namespace Test\Ecotone\EventSourcing\Integration;


use Ecotone\EventSourcing\EventMapper;
use Ecotone\EventSourcing\EventSourcingConfiguration;
use Ecotone\EventSourcing\LazyProophEventStore;
use Ecotone\EventSourcing\EventSourcingRepositoryBuilder;
use Ecotone\Messaging\Config\InMemoryChannelResolver;
use Ecotone\Messaging\Conversion\ConversionService;
use Ecotone\Messaging\Conversion\InMemoryConversionService;
use Ecotone\Messaging\Conversion\MediaType;
use Ecotone\Messaging\Handler\TypeDescriptor;
use Ecotone\Messaging\MessageHeaders;
use Ecotone\Modelling\EventStream;
use Ramsey\Uuid\Uuid;
use Test\Ecotone\EventSourcing\EventSourcingMessagingTest;
use Test\Ecotone\EventSourcing\Fixture\Ticket\Event\TicketWasRegistered;
use Test\Ecotone\EventSourcing\Fixture\Ticket\Ticket;

class EventSourcingRepositoryBuilderTest extends EventSourcingMessagingTest
{
    public function test_storing_and_retrieving()
    {
        $proophRepositoryBuilder = EventSourcingRepositoryBuilder::create(
            EventSourcingConfiguration::createWithDefaults()
                ->withPersistenceStrategy(LazyProophEventStore::SINGLE_STREAM_PERSISTENCE)
        );

        $ticketId = Uuid::uuid4()->toString();
        $ticketWasRegisteredEvent = new TicketWasRegistered($ticketId, "Johny", "standard");
        $ticketWasRegisteredEventAsArray = [
            "ticketId" => $ticketId,
            "assignedPerson" => "Johny",
            "ticketType" => "standard"
        ];

        $repository = $proophRepositoryBuilder->build(InMemoryChannelResolver::createEmpty(), $this->getReferenceSearchServiceWithConnection([
            EventMapper::class => EventMapper::createEmpty(),
            ConversionService::REFERENCE_NAME => InMemoryConversionService::createWithoutConversion()
                ->registerInPHPConversion($ticketWasRegisteredEvent, $ticketWasRegisteredEventAsArray)
                ->registerInPHPConversion($ticketWasRegisteredEventAsArray, $ticketWasRegisteredEvent)
        ]));

        $repository->save(["ticketId"=> $ticketId], Ticket::class, [$ticketWasRegisteredEvent], [
            MessageHeaders::MESSAGE_ID => Uuid::uuid4()->toString(),
            MessageHeaders::TIMESTAMP => 1610285647
        ], 0);

        $resultStream = $repository->findBy(Ticket::class, ["ticketId" => $ticketId]);
        $this->assertEquals(1, $resultStream->getAggregateVersion());
        $this->assertEquals($ticketWasRegisteredEvent, $resultStream->getEvents()[0]->getEvent());
    }

    public function test_having_two_streams_for_difference_instances_of_same_aggregate_using_aggregate_stream_strategy()
    {
        $proophRepositoryBuilder = EventSourcingRepositoryBuilder::create(
            EventSourcingConfiguration::createWithDefaults()
                ->withPersistenceStrategy(LazyProophEventStore::AGGREGATE_STREAM_PERSISTENCE)
        );

        $firstTicketAggregate = Uuid::uuid4()->toString();
        $secondTicketAggregate = Uuid::uuid4()->toString();
        $firstTicketWasRegisteredEvent = new TicketWasRegistered($firstTicketAggregate, "Johny", "standard");
        $firstTicketWasRegisteredEventAsArray = [
            "ticketId" => $firstTicketAggregate,
            "assignedPerson" => "Johny",
            "ticketType" => "standard"
        ];
        $secondTicketWasRegisteredEvent = new TicketWasRegistered($secondTicketAggregate, "Johny", "standard");
        $secondTicketWasRegisteredEventAsArray = [
            "ticketId" => $secondTicketAggregate,
            "assignedPerson" => "Johny",
            "ticketType" => "standard"
        ];

        $repository = $proophRepositoryBuilder->build(InMemoryChannelResolver::createEmpty(), $this->getReferenceSearchServiceWithConnection([
            EventMapper::class => EventMapper::createEmpty(),
            ConversionService::REFERENCE_NAME => InMemoryConversionService::createWithoutConversion()
                ->registerInPHPConversion($firstTicketWasRegisteredEvent, $firstTicketWasRegisteredEventAsArray)
                ->registerInPHPConversion($firstTicketWasRegisteredEventAsArray, $firstTicketWasRegisteredEvent)
                ->registerInPHPConversion($secondTicketWasRegisteredEvent, $secondTicketWasRegisteredEventAsArray)
                ->registerInPHPConversion($secondTicketWasRegisteredEventAsArray, $secondTicketWasRegisteredEvent)
        ]));

        $repository->save(["ticketId"=> $firstTicketAggregate], Ticket::class, [$firstTicketWasRegisteredEvent], [
            MessageHeaders::MESSAGE_ID => Uuid::uuid4()->toString(),
            MessageHeaders::TIMESTAMP => 1610285647
        ], 0);

        $repository->save(["ticketId"=> $secondTicketAggregate], Ticket::class, [$secondTicketWasRegisteredEvent], [
            MessageHeaders::MESSAGE_ID => Uuid::uuid4()->toString(),
            MessageHeaders::TIMESTAMP => 1610285647
        ], 0);

        $resultStream = $repository->findBy(Ticket::class, ["ticketId"=> $firstTicketAggregate]);
        $this->assertEquals(1, $resultStream->getAggregateVersion());
        $this->assertEquals($firstTicketWasRegisteredEvent, $resultStream->getEvents()[0]->getEvent());

        $resultStream = $repository->findBy(Ticket::class, ["ticketId"=> $secondTicketAggregate]);
        $this->assertEquals(1, $resultStream->getAggregateVersion());
        $this->assertEquals($secondTicketWasRegisteredEvent, $resultStream->getEvents()[0]->getEvent());
    }

    public function test_having_two_streams_for_difference_instances_of_same_aggregate_using_single_stream_strategy()
    {
        $proophRepositoryBuilder = EventSourcingRepositoryBuilder::create(
            EventSourcingConfiguration::createWithDefaults()
                ->withPersistenceStrategy(LazyProophEventStore::SINGLE_STREAM_PERSISTENCE)
        );

        $firstTicketAggregate = Uuid::uuid4()->toString();
        $secondTicketAggregate = Uuid::uuid4()->toString();
        $firstTicketWasRegisteredEvent = new TicketWasRegistered($firstTicketAggregate, "Johny", "standard");
        $firstTicketWasRegisteredEventAsArray = [
            "ticketId" => $firstTicketAggregate,
            "assignedPerson" => "Johny",
            "ticketType" => "standard"
        ];
        $secondTicketWasRegisteredEvent = new TicketWasRegistered($secondTicketAggregate, "Johny", "standard");
        $secondTicketWasRegisteredEventAsArray = [
            "ticketId" => $secondTicketAggregate,
            "assignedPerson" => "Johny",
            "ticketType" => "standard"
        ];

        $repository = $proophRepositoryBuilder->build(InMemoryChannelResolver::createEmpty(), $this->getReferenceSearchServiceWithConnection([
            EventMapper::class => EventMapper::createEmpty(),
            ConversionService::REFERENCE_NAME => InMemoryConversionService::createWithoutConversion()
                ->registerInPHPConversion($firstTicketWasRegisteredEvent, $firstTicketWasRegisteredEventAsArray)
                ->registerInPHPConversion($firstTicketWasRegisteredEventAsArray, $firstTicketWasRegisteredEvent)
                ->registerInPHPConversion($secondTicketWasRegisteredEvent, $secondTicketWasRegisteredEventAsArray)
                ->registerInPHPConversion($secondTicketWasRegisteredEventAsArray, $secondTicketWasRegisteredEvent)
        ]));

        $repository->save(["ticketId"=> $firstTicketAggregate], Ticket::class, [$firstTicketWasRegisteredEvent], [
            MessageHeaders::MESSAGE_ID => Uuid::uuid4()->toString(),
            MessageHeaders::TIMESTAMP => 1610285647
        ], 0);

        $repository->save(["ticketId"=> $secondTicketAggregate], Ticket::class, [$secondTicketWasRegisteredEvent], [
            MessageHeaders::MESSAGE_ID => Uuid::uuid4()->toString(),
            MessageHeaders::TIMESTAMP => 1610285647
        ], 0);

        $resultStream = $repository->findBy(Ticket::class, ["ticketId"=> $firstTicketAggregate]);
        $this->assertEquals(1, $resultStream->getAggregateVersion());
        $this->assertEquals($firstTicketWasRegisteredEvent, $resultStream->getEvents()[0]->getEvent());

        $resultStream = $repository->findBy(Ticket::class, ["ticketId"=> $secondTicketAggregate]);
        $this->assertEquals(1, $resultStream->getAggregateVersion());
        $this->assertEquals($secondTicketWasRegisteredEvent, $resultStream->getEvents()[0]->getEvent());
    }
}