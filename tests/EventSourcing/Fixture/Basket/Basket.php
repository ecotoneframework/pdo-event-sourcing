<?php

namespace Test\Ecotone\EventSourcing\Fixture\Basket;

use Ecotone\EventSourcing\Attribute\Stream;
use Ecotone\Modelling\Attribute\AggregateFactory;
use Ecotone\Modelling\Attribute\AggregateIdentifier;
use Ecotone\Modelling\Attribute\CommandHandler;
use Ecotone\Modelling\Attribute\EventSourcingAggregate;
use Ecotone\Modelling\Attribute\EventSourcingHandler;
use Ecotone\Modelling\WithAggregateEvents;
use Ecotone\Modelling\WithAggregateVersioning;
use Test\Ecotone\EventSourcing\Fixture\Basket\Command\AddProduct;
use Test\Ecotone\EventSourcing\Fixture\Basket\Command\CreateBasket;
use Test\Ecotone\EventSourcing\Fixture\Basket\Event\BasketWasCreated;
use Test\Ecotone\EventSourcing\Fixture\Basket\Event\ProductWasAddedToBasket;
use Test\Ecotone\EventSourcing\Fixture\Ticket\Event\AssignedPersonWasChanged;
use Test\Ecotone\EventSourcing\Fixture\Ticket\Event\TicketWasRegistered;
use Test\Ecotone\EventSourcing\Fixture\Ticket\Ticket;

#[EventSourcingAggregate(true)]
#[Stream(self::BASKET_STREAM)]
class Basket
{
    const BASKET_STREAM = "basket_stream";

    use WithAggregateEvents;
    use WithAggregateVersioning;

    #[AggregateIdentifier]
    private string $id;

    #[CommandHandler]
    public static function create(CreateBasket $command) : static
    {
        $basket = new static();
        $basket->recordThat(new BasketWasCreated($command->getId()));

        return $basket;
    }

    #[CommandHandler]
    public function addProduct(AddProduct $command) : void
    {
        $this->recordThat(new ProductWasAddedToBasket($this->id, $command->getProductName()));
    }

    #[EventSourcingHandler]
    public function applyBasketWasCreated(BasketWasCreated $basketWasCreated): void
    {
        $this->id = $basketWasCreated->getId();
    }
}