<?php


namespace Ecotone\EventSourcing\Config;


use Ecotone\EventSourcing\EventSourcingConfiguration;
use Ecotone\EventSourcing\LazyProophProjectionManager;
use Ecotone\Messaging\Handler\ChannelResolver;
use Ecotone\Messaging\Handler\InputOutputMessageHandlerBuilder;
use Ecotone\Messaging\Handler\InterfaceToCall;
use Ecotone\Messaging\Handler\InterfaceToCallRegistry;
use Ecotone\Messaging\Handler\ParameterConverterBuilder;
use Ecotone\Messaging\Handler\ReferenceSearchService;
use Ecotone\Messaging\Handler\ServiceActivator\ServiceActivatorBuilder;
use Ecotone\Messaging\MessageHandler;

class ProjectionManagerBuilder extends InputOutputMessageHandlerBuilder
{
    private string $methodName;
    private EventSourcingConfiguration $eventSourcingConfiguration;
    private array $parameterConverters;

    private function __construct(string $methodName, array $parameterConverters, EventSourcingConfiguration $eventSourcingConfiguration)
    {
        $this->methodName = $methodName;
        $this->parameterConverters = $parameterConverters;
        $this->eventSourcingConfiguration = $eventSourcingConfiguration;
    }

    /**
     * @param ParameterConverterBuilder[] $parameterConverters
     */
    public static function create(string $methodName, array $parameterConverters, EventSourcingConfiguration $eventSourcingConfiguration) : static
    {
        return new self($methodName, $parameterConverters, $eventSourcingConfiguration);
    }

    public function getInputMessageChannelName() : string
    {
        return $this->eventSourcingConfiguration->getProjectManagerReferenceName() . $this->methodName;
    }

    public function getInterceptedInterface(InterfaceToCallRegistry $interfaceToCallRegistry): InterfaceToCall
    {
        return $interfaceToCallRegistry->getFor(LazyProophProjectionManager::class, $this->methodName);
    }

    public function build(ChannelResolver $channelResolver, ReferenceSearchService $referenceSearchService): MessageHandler
    {
        return ServiceActivatorBuilder::createWithDirectReference(
            new LazyProophProjectionManager($this->eventSourcingConfiguration, $referenceSearchService),
            $this->methodName
        )
            ->withMethodParameterConverters($this->parameterConverters)
            ->withInputChannelName($this->getInputMessageChannelName())
            ->build($channelResolver, $referenceSearchService);
    }

    public function resolveRelatedInterfaces(InterfaceToCallRegistry $interfaceToCallRegistry): iterable
    {
        return [$interfaceToCallRegistry->getFor(LazyProophProjectionManager::class, $this->methodName)];
    }

    public function getRequiredReferenceNames(): array
    {
        return [];
    }
}
