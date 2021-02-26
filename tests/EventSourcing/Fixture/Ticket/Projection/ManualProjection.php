<?php

namespace Test\Ecotone\EventSourcing\Fixture\Ticket\Projection;

use Ecotone\EventSourcing\ProjectionConfiguration;
use Ecotone\Messaging\Attribute\ServiceContext;

class ManualProjection
{
    #[ServiceContext]
    public function configureProjection()
    {
        return ;
    }
}