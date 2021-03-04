<?php


namespace Ecotone\EventSourcing\Config\InboundChannelAdapter;

use Ecotone\EventSourcing\EventSourcingConfiguration;
use Ecotone\EventSourcing\LazyProophProjectionManager;
use Ecotone\EventSourcing\ProjectionConfiguration;
use Ecotone\Messaging\Gateway\MessagingEntrypoint;
use Ecotone\Messaging\Handler\ChannelResolver;
use Ecotone\Messaging\Handler\InputOutputMessageHandlerBuilder;
use Ecotone\Messaging\Handler\InterfaceToCall;
use Ecotone\Messaging\Handler\InterfaceToCallRegistry;
use Ecotone\Messaging\Handler\MessageHandlerBuilder;
use Ecotone\Messaging\Handler\Processor\MethodInvoker\Converter\ReferenceBuilder;
use Ecotone\Messaging\Handler\ReferenceSearchService;
use Ecotone\Messaging\Handler\ServiceActivator\ServiceActivatorBuilder;
use Ecotone\Messaging\MessageHandler;

class ProjectionExecutorBuilder extends InputOutputMessageHandlerBuilder implements MessageHandlerBuilder
{
    private EventSourcingConfiguration $eventSourcingConfiguration;
    private ProjectionConfiguration $projectionConfiguration;

    public function __construct(EventSourcingConfiguration $eventSourcingConfiguration, ProjectionConfiguration $projectionConfiguration)
    {
        $this->eventSourcingConfiguration = $eventSourcingConfiguration;
        $this->projectionConfiguration = $projectionConfiguration;
    }

    public function getInterceptedInterface(InterfaceToCallRegistry $interfaceToCallRegistry): InterfaceToCall
    {
        return $interfaceToCallRegistry->getFor(ProjectionExecutor::class, "execute");
    }

    public function build(ChannelResolver $channelResolver, ReferenceSearchService $referenceSearchService): MessageHandler
    {
        return ServiceActivatorBuilder::createWithDirectReference(
            new ProjectionExecutor(
                new LazyProophProjectionManager($this->eventSourcingConfiguration, $referenceSearchService),
                $this->projectionConfiguration
            ),
            "execute"
        )
            ->withMethodParameterConverters([ReferenceBuilder::create("messagingEntrypoint", MessagingEntrypoint::class)])
            ->build($channelResolver, $referenceSearchService);
    }

    public function resolveRelatedInterfaces(InterfaceToCallRegistry $interfaceToCallRegistry): iterable
    {
        return [
            $interfaceToCallRegistry->getFor(ProjectionExecutor::class, "execute")
        ];
    }

    public function getRequiredReferenceNames(): array
    {
        return [];
    }
}