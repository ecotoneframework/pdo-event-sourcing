<?php


namespace Ecotone\EventSourcing\Config;

use Ecotone\AnnotationFinder\AnnotationFinder;
use Ecotone\EventSourcing\Attribute\Projection;
use Ecotone\EventSourcing\Attribute\ProjectionDelete;
use Ecotone\EventSourcing\Attribute\ProjectionInitialization;
use Ecotone\EventSourcing\Attribute\ProjectionReset;
use Ecotone\EventSourcing\Config\InboundChannelAdapter\ProjectionChannelAdapter;
use Ecotone\EventSourcing\Config\InboundChannelAdapter\ProjectionExecutorBuilder;
use Ecotone\EventSourcing\EventMapper;
use Ecotone\EventSourcing\EventSourcingConfiguration;
use Ecotone\EventSourcing\EventStore;
use Ecotone\EventSourcing\ProjectionConfiguration;
use Ecotone\EventSourcing\ProjectionLifeCycleConfiguration;
use Ecotone\EventSourcing\ProjectionManager;
use Ecotone\EventSourcing\EventSourcingRepositoryBuilder;
use Ecotone\Messaging\Attribute\ModuleAnnotation;
use Ecotone\Messaging\Config\Annotation\AnnotatedDefinitionReference;
use Ecotone\Messaging\Config\Annotation\ModuleConfiguration\NoExternalConfigurationModule;
use Ecotone\Messaging\Config\Configuration;
use Ecotone\Messaging\Config\ConsoleCommandConfiguration;
use Ecotone\Messaging\Config\ConsoleCommandParameter;
use Ecotone\Messaging\Config\ModuleReferenceSearchService;
use Ecotone\Messaging\Endpoint\InboundChannelAdapter\InboundChannelAdapterBuilder;
use Ecotone\Messaging\Handler\ClassDefinition;
use Ecotone\Messaging\Handler\Gateway\GatewayProxyBuilder;
use Ecotone\Messaging\Handler\Gateway\ParameterToMessageConverter\GatewayHeaderBuilder;
use Ecotone\Messaging\Handler\Gateway\ParameterToMessageConverter\GatewayPayloadBuilder;
use Ecotone\Messaging\Handler\Processor\MethodInvoker\Converter\HeaderBuilder;
use Ecotone\Messaging\Handler\Processor\MethodInvoker\Converter\PayloadBuilder;
use Ecotone\Messaging\Handler\ServiceActivator\ServiceActivatorBuilder;
use Ecotone\Messaging\Handler\TypeDescriptor;
use Ecotone\Messaging\Support\Assert;
use Ecotone\Modelling\Attribute\EventHandler;
use Ecotone\Modelling\Config\ModellingHandlerModule;
use Prooph\EventStore\Projection\ReadModel;
use Ramsey\Uuid\Uuid;

#[ModuleAnnotation]
class EventSourcingModule extends NoExternalConfigurationModule
{
    /**
     * @var ProjectionConfiguration[]
     */
    private array $projectionConfigurations;
    /** @var ServiceActivatorBuilder[]  */
    private array $projectionLifeCycleServiceActivators = [];

    /**
     * @var ProjectionConfiguration[]
     * @var ServiceActivatorBuilder[]
     */
    public function __construct(array $projectionConfigurations, array $projectionLifeCycleServiceActivators)
    {
        $this->projectionConfigurations = $projectionConfigurations;
        $this->projectionLifeCycleServiceActivators = $projectionLifeCycleServiceActivators;
    }

    public static function create(AnnotationFinder $annotationRegistrationService): static
    {
        $projectionClassNames = $annotationRegistrationService->findAnnotatedClasses(Projection::class);
        $projectionEventHandlers = $annotationRegistrationService->findCombined(Projection::class, EventHandler::class);
        $projectionConfigurations = [];
        $projectionLifeCyclesServiceActivators = [];

        foreach ($projectionClassNames as $projectionClassName) {
            $projectionLifeCycle = ProjectionLifeCycleConfiguration::create();

            $classDefinition = ClassDefinition::createUsingAnnotationParser(TypeDescriptor::create($projectionClassName), $annotationRegistrationService);
            $methods = [];
            $projectionInitialization = TypeDescriptor::create(ProjectionInitialization::class);
            $projectionDelete = TypeDescriptor::create(ProjectionDelete::class);
            $projectionReset = TypeDescriptor::create(ProjectionReset::class);
            foreach ($classDefinition->getPublicMethodNames() as $publicMethodName) {
                foreach ($annotationRegistrationService->getAnnotationsForMethod($projectionClassName, $publicMethodName) as $attribute) {
                    $attributeType = TypeDescriptor::createFromVariable($attribute);
                    if ($attributeType->equals($projectionInitialization)) {
                        $requestChannel = Uuid::uuid4()->toString();
                        $projectionLifeCycle = $projectionLifeCycle->withInitializationRequestChannel($requestChannel);
                        $projectionLifeCyclesServiceActivators[] = ServiceActivatorBuilder::create(
                            AnnotatedDefinitionReference::getReferenceForClassName($annotationRegistrationService, $projectionClassName),
                            $publicMethodName
                        )->withInputChannelName($requestChannel);
                    }
                    if ($attributeType->equals($projectionDelete)) {
                        $requestChannel = Uuid::uuid4()->toString();
                        $projectionLifeCycle = $projectionLifeCycle->withDeleteRequestChannel($requestChannel);
                        $projectionLifeCyclesServiceActivators[] = ServiceActivatorBuilder::create(
                            AnnotatedDefinitionReference::getReferenceForClassName($annotationRegistrationService, $projectionClassName),
                            $publicMethodName
                        )->withInputChannelName($requestChannel);
                    }
                    if ($attributeType->equals($projectionReset)) {
                        $requestChannel = Uuid::uuid4()->toString();
                        $projectionLifeCycle = $projectionLifeCycle->withResetRequestChannel($requestChannel);
                        $projectionLifeCyclesServiceActivators[] = ServiceActivatorBuilder::create(
                            AnnotatedDefinitionReference::getReferenceForClassName($annotationRegistrationService, $projectionClassName),
                            $publicMethodName
                        )->withInputChannelName($requestChannel);
                    }
                }
            }

            $attributes = $annotationRegistrationService->getAnnotationsForClass($projectionClassName);
            $projectionAttribute = null;
            foreach ($attributes as $attribute) {
                if ($attribute instanceof Projection) {
                    $projectionAttribute = $attribute;
                    break;
                }
            }

            Assert::keyNotExists($projectionConfigurations, $projectionAttribute->getName(), "Can't define projection with name {$projectionAttribute->getName()} twice");

            if ($projectionAttribute->isFromAll()) {
                $projectionConfiguration = ProjectionConfiguration::fromAll(
                    $projectionAttribute->getName(),
                    $projectionLifeCycle
                );
            }else if ($projectionAttribute->getFromStreams()){
                $projectionConfiguration = ProjectionConfiguration::fromStreams(
                    $projectionAttribute->getName(),
                    $projectionLifeCycle,
                    ...$projectionAttribute->getFromStreams()
                );
            }else {
                $projectionConfiguration = ProjectionConfiguration::fromCategories(
                    $projectionAttribute->getName(),
                    $projectionLifeCycle,
                    ...$projectionAttribute->getFromCategories()
                );
            }

            $projectionConfigurations[$projectionAttribute->getName()] = $projectionConfiguration
                                            ->withOptions($projectionAttribute->getOptions());
        }

        foreach ($projectionEventHandlers as $projectionEventHandler) {
            /** @var Projection $projectionAttribute */
            $projectionAttribute = $projectionEventHandler->getAnnotationForClass();
            $projectionConfiguration = $projectionConfigurations[$projectionAttribute->getName()];

            $projectionConfigurations[$projectionAttribute->getName()] = $projectionConfiguration->withProjectionEventHandler(
                ModellingHandlerModule::getNamedMessageChannelForEventHandler($projectionEventHandler),
                ModellingHandlerModule::getHandlerChannel($projectionEventHandler)
            );
        }

        return new self($projectionConfigurations, $projectionLifeCyclesServiceActivators);
    }

    public function prepare(Configuration $configuration, array $extensionObjects, ModuleReferenceSearchService $moduleReferenceSearchService): void
    {
        $moduleReferenceSearchService->store(EventMapper::class, EventMapper::createEmpty());
        $eventSourcingConfiguration = EventSourcingConfiguration::createWithDefaults();
        foreach ($extensionObjects as $extensionObject) {
            if ($extensionObject instanceof EventSourcingConfiguration) {
                $eventSourcingConfiguration = $extensionObject;
            }
        }

        $lazyProophProjectionManager = null;
        foreach ($this->projectionConfigurations as $projectionConfiguration) {
            $generatedChannelName = Uuid::uuid4()->toString();
            $messageHandlerBuilder = new ProjectionExecutorBuilder($eventSourcingConfiguration, $projectionConfiguration);
            $messageHandlerBuilder = $messageHandlerBuilder->withInputChannelName($generatedChannelName);
            $configuration->registerMessageHandler($messageHandlerBuilder);

            $configuration->registerConsumer(InboundChannelAdapterBuilder::createWithDirectObject(
                $generatedChannelName,
                new ProjectionChannelAdapter(),
                "run"
            )->withEndpointId($projectionConfiguration->getProjectionName()));
        }
        foreach ($this->projectionLifeCycleServiceActivators as $serviceActivator) {
            $configuration->registerMessageHandler($serviceActivator);
        }

        $this->registerEventStore($configuration, $eventSourcingConfiguration);
        $this->registerProjectionManager($configuration, $eventSourcingConfiguration);
    }

    private function registerEventStore(Configuration $configuration, EventSourcingConfiguration $eventSourcingConfiguration) : void
    {
        $this->registerEventStoreAction(
            "updateStreamMetadata",
            [HeaderBuilder::create("streamName", "ecotone.eventSourcing.eventStore.streamName"), HeaderBuilder::create("newMetadata", "ecotone.eventSourcing.eventStore.newMetadata")],
            [GatewayHeaderBuilder::create("streamName", "ecotone.eventSourcing.eventStore.streamName"), GatewayHeaderBuilder::create("newMetadata", "ecotone.eventSourcing.eventStore.newMetadata")],
            $eventSourcingConfiguration,
            $configuration
        );

        $this->registerEventStoreAction(
            "create",
            [HeaderBuilder::create("streamName", "ecotone.eventSourcing.eventStore.streamName"), PayloadBuilder::create("streamEvents"), HeaderBuilder::create("streamMetadata", "ecotone.eventSourcing.eventStore.streamMetadata")],
            [GatewayHeaderBuilder::create("streamName", "ecotone.eventSourcing.eventStore.streamName"), GatewayPayloadBuilder::create("streamEvents"), GatewayHeaderBuilder::create("streamMetadata", "ecotone.eventSourcing.eventStore.streamMetadata")],
            $eventSourcingConfiguration,
            $configuration
        );

        $this->registerEventStoreAction(
            "appendTo",
            [HeaderBuilder::create("streamName", "ecotone.eventSourcing.eventStore.streamName"), PayloadBuilder::create("streamEvents")],
            [GatewayHeaderBuilder::create("streamName", "ecotone.eventSourcing.eventStore.streamName"), GatewayPayloadBuilder::create("streamEvents")],
            $eventSourcingConfiguration,
            $configuration
        );

        $this->registerEventStoreAction(
            "delete",
            [HeaderBuilder::create("streamName", "ecotone.eventSourcing.eventStore.streamName")],
            [GatewayHeaderBuilder::create("streamName", "ecotone.eventSourcing.eventStore.streamName")],
            $eventSourcingConfiguration,
            $configuration
        );

        $this->registerEventStoreAction(
            "fetchStreamMetadata",
            [HeaderBuilder::create("streamName", "ecotone.eventSourcing.eventStore.streamName")],
            [GatewayHeaderBuilder::create("streamName", "ecotone.eventSourcing.eventStore.streamName")],
            $eventSourcingConfiguration,
            $configuration
        );

        $this->registerEventStoreAction(
            "hasStream",
            [HeaderBuilder::create("streamName", "ecotone.eventSourcing.eventStore.streamName")],
            [GatewayHeaderBuilder::create("streamName", "ecotone.eventSourcing.eventStore.streamName")],
            $eventSourcingConfiguration,
            $configuration
        );

        $this->registerEventStoreAction(
            "load",
            [HeaderBuilder::create("streamName", "ecotone.eventSourcing.eventStore.streamName"), HeaderBuilder::create("fromNumber", "ecotone.eventSourcing.eventStore.fromNumber"), HeaderBuilder::createOptional("count", "ecotone.eventSourcing.eventStore.count"), PayloadBuilder::create("metadataMatcher"), HeaderBuilder::create("deserialize", "ecotone.eventSourcing.eventStore.deserialize")],
            [GatewayHeaderBuilder::create("streamName", "ecotone.eventSourcing.eventStore.streamName"), GatewayHeaderBuilder::create("fromNumber", "ecotone.eventSourcing.eventStore.fromNumber"), GatewayHeaderBuilder::create("count", "ecotone.eventSourcing.eventStore.count"), GatewayPayloadBuilder::create("metadataMatcher"), GatewayHeaderBuilder::create("deserialize", "ecotone.eventSourcing.eventStore.deserialize")],
            $eventSourcingConfiguration,
            $configuration
        );

        $this->registerEventStoreAction(
            "loadReverse",
            [HeaderBuilder::create("streamName", "ecotone.eventSourcing.eventStore.streamName"), HeaderBuilder::createOptional("fromNumber", "ecotone.eventSourcing.eventStore.fromNumber"), HeaderBuilder::createOptional("count", "ecotone.eventSourcing.eventStore.count"), PayloadBuilder::create("metadataMatcher"), HeaderBuilder::create("deserialize", "ecotone.eventSourcing.eventStore.deserialize")],
            [GatewayHeaderBuilder::create("streamName", "ecotone.eventSourcing.eventStore.streamName"), GatewayHeaderBuilder::create("fromNumber", "ecotone.eventSourcing.eventStore.fromNumber"), GatewayHeaderBuilder::create("count", "ecotone.eventSourcing.eventStore.count"), GatewayPayloadBuilder::create("metadataMatcher"), GatewayHeaderBuilder::create("deserialize", "ecotone.eventSourcing.eventStore.deserialize")],
            $eventSourcingConfiguration,
            $configuration
        );

        $this->registerEventStoreAction(
            "fetchStreamNames",
            [HeaderBuilder::createOptional("filter", "ecotone.eventSourcing.eventStore.filter"), PayloadBuilder::create("metadataMatcher"), HeaderBuilder::create("limit", "ecotone.eventSourcing.eventStore.limit"), HeaderBuilder::create("offset", "ecotone.eventSourcing.eventStore.offset")],
            [GatewayHeaderBuilder::create("filter", "ecotone.eventSourcing.eventStore.filter"), GatewayPayloadBuilder::create("metadataMatcher"), GatewayHeaderBuilder::create("limit", "ecotone.eventSourcing.eventStore.limit"), GatewayHeaderBuilder::create("offset", "ecotone.eventSourcing.eventStore.offset")],
            $eventSourcingConfiguration,
            $configuration
        );

        $this->registerEventStoreAction(
            "fetchStreamNamesRegex",
            [HeaderBuilder::createOptional("filter", "ecotone.eventSourcing.eventStore.filter"), PayloadBuilder::create("metadataMatcher"), HeaderBuilder::create("limit", "ecotone.eventSourcing.eventStore.limit"), HeaderBuilder::create("offset", "ecotone.eventSourcing.eventStore.offset")],
            [GatewayHeaderBuilder::create("filter", "ecotone.eventSourcing.eventStore.filter"), GatewayPayloadBuilder::create("metadataMatcher"), GatewayHeaderBuilder::create("limit", "ecotone.eventSourcing.eventStore.limit"), GatewayHeaderBuilder::create("offset", "ecotone.eventSourcing.eventStore.offset")],
            $eventSourcingConfiguration,
            $configuration
        );

        $this->registerEventStoreAction(
            "fetchCategoryNames",
            [HeaderBuilder::createOptional("filter", "ecotone.eventSourcing.eventStore.filter"), HeaderBuilder::create("limit", "ecotone.eventSourcing.eventStore.limit"), HeaderBuilder::create("offset", "ecotone.eventSourcing.eventStore.offset")],
            [GatewayHeaderBuilder::create("filter", "ecotone.eventSourcing.eventStore.filter"), GatewayHeaderBuilder::create("limit", "ecotone.eventSourcing.eventStore.limit"), GatewayHeaderBuilder::create("offset", "ecotone.eventSourcing.eventStore.offset")],
            $eventSourcingConfiguration,
            $configuration
        );

        $this->registerEventStoreAction(
            "fetchCategoryNamesRegex",
            [HeaderBuilder::createOptional("filter", "ecotone.eventSourcing.eventStore.filter"), HeaderBuilder::create("limit", "ecotone.eventSourcing.eventStore.limit"), HeaderBuilder::create("offset", "ecotone.eventSourcing.eventStore.offset")],
            [GatewayHeaderBuilder::create("filter", "ecotone.eventSourcing.eventStore.filter"), GatewayHeaderBuilder::create("limit", "ecotone.eventSourcing.eventStore.limit"), GatewayHeaderBuilder::create("offset", "ecotone.eventSourcing.eventStore.offset")],
            $eventSourcingConfiguration,
            $configuration
        );
    }

    private function registerProjectionManager(Configuration $configuration, EventSourcingConfiguration $eventSourcingConfiguration) : void
    {
        $this->registerProjectionManagerAction("createQuery", [], [], $eventSourcingConfiguration, $configuration);

        $this->registerProjectionManagerAction(
            "createProjection",
            [HeaderBuilder::create("name", "ecotone.eventSourcing.manager.name"), HeaderBuilder::create("options", "ecotone.eventSourcing.manager.options")],
            [GatewayHeaderBuilder::create("name", "ecotone.eventSourcing.manager.name"), GatewayHeaderBuilder::create("options", "ecotone.eventSourcing.manager.options")],
            $eventSourcingConfiguration,
            $configuration
        );

        $this->registerProjectionManagerAction(
            "createReadModelProjection",
            [HeaderBuilder::create("name", "ecotone.eventSourcing.manager.name"), PayloadBuilder::create("readModel"), HeaderBuilder::create("options", "ecotone.eventSourcing.manager.options")],
            [GatewayHeaderBuilder::create("name", "ecotone.eventSourcing.manager.name"), GatewayPayloadBuilder::create("readModel"), GatewayHeaderBuilder::create("options", "ecotone.eventSourcing.manager.options")],
            $eventSourcingConfiguration,
            $configuration
        );

        $this->registerProjectionManagerAction(
            "deleteProjection",
            [HeaderBuilder::create("name", "ecotone.eventSourcing.manager.name"), HeaderBuilder::create("deleteEmittedEvents", "ecotone.eventSourcing.manager.deleteEmittedEvents")],
            [GatewayHeaderBuilder::create("name", "ecotone.eventSourcing.manager.name"), GatewayHeaderBuilder::create("deleteEmittedEvents", "ecotone.eventSourcing.manager.deleteEmittedEvents")],
            $eventSourcingConfiguration,
            $configuration,
            "ecotone:es:delete-projection",
            [ConsoleCommandParameter::create("name"), ConsoleCommandParameter::createWithDefaultValue("deleteEmittedEvents", true)]
        );

        $this->registerProjectionManagerAction(
            "resetProjection",
            [HeaderBuilder::create("name", "ecotone.eventSourcing.manager.name")],
            [GatewayHeaderBuilder::create("name", "ecotone.eventSourcing.manager.name")],
            $eventSourcingConfiguration,
            $configuration,
            "ecotone:es:reset-projection",
            [ConsoleCommandParameter::create("name")]
        );

        $this->registerProjectionManagerAction(
            "stopProjection",
            [HeaderBuilder::create("name", "ecotone.eventSourcing.manager.name")],
            [GatewayHeaderBuilder::create("name", "ecotone.eventSourcing.manager.name")],
            $eventSourcingConfiguration,
            $configuration,
            "ecotone:es:stop-projection",
            [ConsoleCommandParameter::create("name")]
        );

        $this->registerProjectionManagerAction(
            "fetchProjectionNames",
            [HeaderBuilder::createOptional("filter", "ecotone.eventSourcing.manager.filter"), HeaderBuilder::create("limit", "ecotone.eventSourcing.manager.limit"), HeaderBuilder::create("offset", "ecotone.eventSourcing.manager.offset")],
            [GatewayHeaderBuilder::create("filter", "ecotone.eventSourcing.manager.filter"), GatewayHeaderBuilder::create("limit", "ecotone.eventSourcing.manager.limit"), GatewayHeaderBuilder::create("offset", "ecotone.eventSourcing.manager.offset")],
            $eventSourcingConfiguration,
            $configuration
        );

        $this->registerProjectionManagerAction(
            "fetchProjectionNamesRegex",
            [HeaderBuilder::create("regex", "ecotone.eventSourcing.manager.regex"), HeaderBuilder::create("limit", "ecotone.eventSourcing.manager.limit"), HeaderBuilder::create("offset", "ecotone.eventSourcing.manager.offset")],
            [GatewayHeaderBuilder::create("regex", "ecotone.eventSourcing.manager.regex"), GatewayHeaderBuilder::create("limit", "ecotone.eventSourcing.manager.limit"), GatewayHeaderBuilder::create("offset", "ecotone.eventSourcing.manager.offset")],
            $eventSourcingConfiguration,
            $configuration
        );

        $this->registerProjectionManagerAction(
            "fetchProjectionStatus",
            [HeaderBuilder::create("name", "ecotone.eventSourcing.manager.name")],
            [GatewayHeaderBuilder::create("name", "ecotone.eventSourcing.manager.name")],
            $eventSourcingConfiguration,
            $configuration
        );

        $this->registerProjectionManagerAction(
            "fetchProjectionStreamPositions",
            [HeaderBuilder::create("name", "ecotone.eventSourcing.manager.name")],
            [GatewayHeaderBuilder::create("name", "ecotone.eventSourcing.manager.name")],
            $eventSourcingConfiguration,
            $configuration
        );

        $this->registerProjectionManagerAction(
            "fetchProjectionState",
            [HeaderBuilder::create("name", "ecotone.eventSourcing.manager.name")],
            [GatewayHeaderBuilder::create("name", "ecotone.eventSourcing.manager.name")],
            $eventSourcingConfiguration,
            $configuration
        );
    }

    public function canHandle($extensionObject): bool
    {
        return $extensionObject instanceof EventSourcingConfiguration;
    }

    public function getModuleExtensions(array $serviceExtensions): array
    {
        foreach ($serviceExtensions as $serviceExtension) {
            if ($serviceExtension instanceof EventSourcingRepositoryBuilder) {
                return [];
            }
        }

        return [EventSourcingRepositoryBuilder::create(EventSourcingConfiguration::createWithDefaults())];
    }

    private function registerProjectionManagerAction(string $methodName, array $endpointConverters, array $gatewayConverters, EventSourcingConfiguration $eventSourcingConfiguration, Configuration $configuration, ?string $consoleCommandName = null, array $consoleCommandParameters = []): void
    {
        $messageHandlerBuilder = ProjectionManagerBuilder::create($methodName, $endpointConverters, $eventSourcingConfiguration);
        $configuration->registerMessageHandler($messageHandlerBuilder);
        $configuration->registerGatewayBuilder(
            GatewayProxyBuilder::create($eventSourcingConfiguration->getProjectManagerReferenceName(), ProjectionManager::class, $methodName, $messageHandlerBuilder->getInputMessageChannelName())
                ->withParameterConverters($gatewayConverters)
        );

        if ($consoleCommandName) {
            $configuration->registerConsoleCommand(ConsoleCommandConfiguration::create(
                $messageHandlerBuilder->getInputMessageChannelName(),
                $consoleCommandName,
                $consoleCommandParameters
            ));
        }
    }

    private function registerEventStoreAction(string $methodName, array $endpointConverters, array $gatewayConverters, EventSourcingConfiguration $eventSourcingConfiguration, Configuration $configuration): void
    {
        $messageHandlerBuilder = EventStoreBuilder::create($methodName, $endpointConverters, $eventSourcingConfiguration);
        $configuration->registerMessageHandler($messageHandlerBuilder);
        $configuration->registerGatewayBuilder(
            GatewayProxyBuilder::create($eventSourcingConfiguration->getProjectManagerReferenceName(), EventStore::class, $methodName, $messageHandlerBuilder->getInputMessageChannelName())
                ->withParameterConverters($gatewayConverters)
        );
    }
}