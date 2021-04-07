<?php


namespace Ecotone\EventSourcing\Config\InboundChannelAdapter;

use Ecotone\EventSourcing\EventSourcingConfiguration;
use Ecotone\EventSourcing\LazyProophProjectionManager;
use Ecotone\EventSourcing\ProjectionSetupConfiguration;
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
    private ProjectionSetupConfiguration $projectionConfiguration;
    private string $methodName;

    public function __construct(EventSourcingConfiguration $eventSourcingConfiguration, ProjectionSetupConfiguration $projectionConfiguration, string $methodName)
    {
        $this->eventSourcingConfiguration = $eventSourcingConfiguration;
        $this->projectionConfiguration = $projectionConfiguration;
        $this->methodName = $methodName;
    }

    public function getInterceptedInterface(InterfaceToCallRegistry $interfaceToCallRegistry): InterfaceToCall
    {
        return $interfaceToCallRegistry->getFor(ProjectionExecutor::class, $this->methodName);
    }

    public function build(ChannelResolver $channelResolver, ReferenceSearchService $referenceSearchService): MessageHandler
    {
        return ServiceActivatorBuilder::createWithDirectReference(
            new ProjectionExecutor(
                new LazyProophProjectionManager($this->eventSourcingConfiguration, $referenceSearchService),
                $this->projectionConfiguration
            ),
            $this->methodName
        )
            ->withOutputMessageChannel($this->getOutputMessageChannelName())
            ->withMethodParameterConverters([ReferenceBuilder::create("messagingEntrypoint", MessagingEntrypoint::class)])
            ->build($channelResolver, $referenceSearchService);
    }

    public function resolveRelatedInterfaces(InterfaceToCallRegistry $interfaceToCallRegistry): iterable
    {
        return [
            $interfaceToCallRegistry->getFor(ProjectionExecutor::class, $this->methodName)
        ];
    }

    public function getRequiredReferenceNames(): array
    {
        return [];
    }
}