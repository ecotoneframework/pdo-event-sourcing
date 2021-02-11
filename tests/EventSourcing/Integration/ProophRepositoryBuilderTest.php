<?php


namespace Test\Ecotone\EventSourcing\Integration;


use Ecotone\EventSourcing\EventMapper;
use Ecotone\EventSourcing\ProophRepositoryBuilder;
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

class ProophRepositoryBuilderTest extends EventSourcingMessagingTest
{
    public function TODOD_test_throwing_exception_if_aggregate_has_no_version_defined()
    {

    }

    public function test_storing_and_retrieving()
    {
        $proophRepositoryBuilder = ProophRepositoryBuilder::create();

        $ticketId = Uuid::uuid4()->toString();
        $repository = $proophRepositoryBuilder->build(InMemoryChannelResolver::createEmpty(), $this->getReferenceSearchServiceWithConnection([
            EventMapper::class => EventMapper::createEmpty(),
            ConversionService::REFERENCE_NAME => InMemoryConversionService::createWithConversion(MediaType::APPLICATION_X_PHP, TicketWasRegistered::class, MediaType::APPLICATION_X_PHP, TypeDescriptor::ARRAY, [
                "ticketId" => $ticketId,
                "assignedPerson" => "Johny",
                "ticketType" => "standard"
            ])
        ]));

        $ticketWasRegisteredEvent = new TicketWasRegistered($ticketId, "Johny", "standard");
        $repository->save(["ticketId"=> $ticketId], Ticket::class, [
            $ticketWasRegisteredEvent
        ], [
            MessageHeaders::MESSAGE_ID => Uuid::uuid4()->toString(),
            MessageHeaders::TIMESTAMP => 1610285647
        ], 0);

        $this->assertEquals(
            EventStream::createWith(1, [$ticketWasRegisteredEvent]),
            $repository->findBy(Ticket::class, ["ticketId"=> $ticketId])
        );
    }
}