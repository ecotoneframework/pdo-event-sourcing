<?php


namespace Ecotone\EventSourcing\Config;

use Ecotone\AnnotationFinder\AnnotationFinder;
use Ecotone\EventSourcing\AggregateStreamMapping;
use Ecotone\EventSourcing\Attribute\EventSourcedEvent;
use Ecotone\EventSourcing\Attribute\Projection;
use Ecotone\EventSourcing\Attribute\ProjectionDelete;
use Ecotone\EventSourcing\Attribute\ProjectionInitialization;
use Ecotone\EventSourcing\Attribute\ProjectionReset;
use Ecotone\EventSourcing\Attribute\Stream;
use Ecotone\EventSourcing\Config\InboundChannelAdapter\ProjectionChannelAdapter;
use Ecotone\EventSourcing\Config\InboundChannelAdapter\ProjectionExecutor;
use Ecotone\EventSourcing\Config\InboundChannelAdapter\ProjectionExecutorBuilder;
use Ecotone\EventSourcing\EventMapper;
use Ecotone\EventSourcing\EventSourcingConfiguration;
use Ecotone\EventSourcing\EventStore;
use Ecotone\EventSourcing\ProjectionRunningConfiguration;
use Ecotone\EventSourcing\ProjectionSetupConfiguration;
use Ecotone\EventSourcing\ProjectionLifeCycleConfiguration;
use Ecotone\EventSourcing\ProjectionManager;
use Ecotone\EventSourcing\EventSourcingRepositoryBuilder;
use Ecotone\Messaging\Attribute\EndpointAnnotation;
use Ecotone\Messaging\Attribute\ModuleAnnotation;
use Ecotone\Messaging\Config\Annotation\AnnotatedDefinitionReference;
use Ecotone\Messaging\Config\Annotation\ModuleConfiguration\AsynchronousModule;
use Ecotone\Messaging\Config\Annotation\ModuleConfiguration\NoExternalConfigurationModule;
use Ecotone\Messaging\Config\BeforeSend\BeforeSendChannelInterceptorBuilder;
use Ecotone\Messaging\Config\Configuration;
use Ecotone\Messaging\Config\ConsoleCommandConfiguration;
use Ecotone\Messaging\Config\ConsoleCommandParameter;
use Ecotone\Messaging\Config\ModuleReferenceSearchService;
use Ecotone\Messaging\Endpoint\InboundChannelAdapter\InboundChannelAdapterBuilder;
use Ecotone\Messaging\Handler\ClassDefinition;
use Ecotone\Messaging\Handler\Gateway\GatewayProxyBuilder;
use Ecotone\Messaging\Handler\Gateway\ParameterToMessageConverter\GatewayHeaderBuilder;
use Ecotone\Messaging\Handler\Gateway\ParameterToMessageConverter\GatewayPayloadBuilder;
use Ecotone\Messaging\Handler\InterfaceToCall;
use Ecotone\Messaging\Handler\Processor\MethodInvoker\Converter\HeaderBuilder;
use Ecotone\Messaging\Handler\Processor\MethodInvoker\Converter\PayloadBuilder;
use Ecotone\Messaging\Handler\Processor\MethodInvoker\Converter\ReferenceBuilder;
use Ecotone\Messaging\Handler\Processor\MethodInvoker\MethodInterceptor;
use Ecotone\Messaging\Handler\ServiceActivator\ServiceActivatorBuilder;
use Ecotone\Messaging\Handler\TypeDescriptor;
use Ecotone\Messaging\Precedence;
use Ecotone\Messaging\Support\Assert;
use Ecotone\Modelling\Attribute\EventHandler;
use Ecotone\Modelling\Attribute\EventSourcingAggregate;
use Ecotone\Modelling\Config\ModellingHandlerModule;
use Laminas\Service;
use Prooph\EventStore\Projection\ReadModel;
use Prooph\EventStore\Projection\ReadModelProjector;
use Ramsey\Uuid\Uuid;

#[ModuleAnnotation]
class EventSourcingModule extends NoExternalConfigurationModule
{
    const ECOTONE_ES_STOP_PROJECTION   = "ecotone:es:stop-projection";
    const ECOTONE_ES_RESET_PROJECTION  = "ecotone:es:reset-projection";
    const ECOTONE_ES_DELETE_PROJECTION = "ecotone:es:delete-projection";
    /**
     * @var ProjectionSetupConfiguration[]
     */
    private array $projectionSetupConfigurations;
    /** @var ServiceActivatorBuilder[] */
    private array $projectionLifeCycleServiceActivators = [];
    private EventMapper $eventMapper;
    private AggregateStreamMapping $aggregateToStreamMapping;
    /** @var InterfaceToCall[] */
    private array $relatedInterfaces = [];
    /** @var string[] */
    private array $requiredReferences = [];

    /**
     * @var ProjectionSetupConfiguration[]
     * @var ServiceActivatorBuilder[]
     */
    private function __construct(array $projectionConfigurations, array $projectionLifeCycleServiceActivators, EventMapper $eventMapper, AggregateStreamMapping $aggregateToStreamMapping, array $relatedInterfaces, array $requiredReferences)
    {
        $this->projectionSetupConfigurations        = $projectionConfigurations;
        $this->projectionLifeCycleServiceActivators = $projectionLifeCycleServiceActivators;
        $this->eventMapper                          = $eventMapper;
        $this->aggregateToStreamMapping             = $aggregateToStreamMapping;
        $this->relatedInterfaces                    = $relatedInterfaces;
        $this->requiredReferences                   = $requiredReferences;
    }

    public static function create(AnnotationFinder $annotationRegistrationService): static
    {
        $fromClassToNameMapping = [];
        $fromNameToClassMapping = [];
        foreach ($annotationRegistrationService->findAnnotatedClasses(EventSourcedEvent::class) as $namedEventClass) {
            /** @var EventSourcedEvent $attribute */
            $attribute = $annotationRegistrationService->getAttributeForClass($namedEventClass, EventSourcedEvent::class);

            $fromClassToNameMapping[$namedEventClass]      = $attribute->getName();
            $fromNameToClassMapping[$attribute->getName()] = $namedEventClass;
        }

        $aggregateToStreamMapping = [];
        foreach ($annotationRegistrationService->findAnnotatedClasses(Stream::class) as $aggregateWithCustomStream) {
            /** @var Stream $attribute */
            $attribute = $annotationRegistrationService->getAttributeForClass($aggregateWithCustomStream, Stream::class);

            $aggregateToStreamMapping[$aggregateWithCustomStream] = $attribute->getName();
        }


        $projectionClassNames                  = $annotationRegistrationService->findAnnotatedClasses(Projection::class);
        $projectionEventHandlers               = $annotationRegistrationService->findCombined(Projection::class, EventHandler::class);
        $projectionSetupConfigurations              = [];
        $projectionLifeCyclesServiceActivators = [];

        $relatedInterfaces  = [];
        $requiredReferences = [];
        foreach ($projectionClassNames as $projectionClassName) {
            $referenceName        = AnnotatedDefinitionReference::getReferenceForClassName($annotationRegistrationService, $projectionClassName);
            $requiredReferences[] = $referenceName;

            $projectionLifeCycle = ProjectionLifeCycleConfiguration::create();

            $classDefinition          = ClassDefinition::createUsingAnnotationParser(TypeDescriptor::create($projectionClassName), $annotationRegistrationService);
            $projectionInitialization = TypeDescriptor::create(ProjectionInitialization::class);
            $projectionDelete         = TypeDescriptor::create(ProjectionDelete::class);
            $projectionReset          = TypeDescriptor::create(ProjectionReset::class);
            foreach ($classDefinition->getPublicMethodNames() as $publicMethodName) {
                $relatedInterfaces[] = InterfaceToCall::create($projectionClassName, $publicMethodName);
                foreach ($annotationRegistrationService->getAnnotationsForMethod($projectionClassName, $publicMethodName) as $attribute) {
                    $attributeType = TypeDescriptor::createFromVariable($attribute);
                    if ($attributeType->equals($projectionInitialization)) {
                        $requestChannel                          = Uuid::uuid4()->toString();
                        $projectionLifeCycle                     = $projectionLifeCycle->withInitializationRequestChannel($requestChannel);
                        $projectionLifeCyclesServiceActivators[] = ServiceActivatorBuilder::create(
                            $referenceName,
                            $publicMethodName
                        )->withInputChannelName($requestChannel);
                    }
                    if ($attributeType->equals($projectionDelete)) {
                        $requestChannel                          = Uuid::uuid4()->toString();
                        $projectionLifeCycle                     = $projectionLifeCycle->withDeleteRequestChannel($requestChannel);
                        $projectionLifeCyclesServiceActivators[] = ServiceActivatorBuilder::create(
                            $referenceName,
                            $publicMethodName
                        )->withInputChannelName($requestChannel);
                    }
                    if ($attributeType->equals($projectionReset)) {
                        $requestChannel                          = Uuid::uuid4()->toString();
                        $projectionLifeCycle                     = $projectionLifeCycle->withResetRequestChannel($requestChannel);
                        $projectionLifeCyclesServiceActivators[] = ServiceActivatorBuilder::create(
                            $referenceName,
                            $publicMethodName
                        )->withInputChannelName($requestChannel);
                    }
                }
            }

            $attributes          = $annotationRegistrationService->getAnnotationsForClass($projectionClassName);
            /** @var Projection $projectionAttribute */
            $projectionAttribute = null;
            foreach ($attributes as $attribute) {
                if ($attribute instanceof Projection) {
                    $projectionAttribute = $attribute;
                    break;
                }
            }

            Assert::keyNotExists($projectionSetupConfigurations, $projectionAttribute->getName(), "Can't define projection with name {$projectionAttribute->getName()} twice");

            if ($projectionAttribute->isFromAll()) {
                $projectionConfiguration = ProjectionSetupConfiguration::fromAll(
                    $projectionAttribute->getName(),
                    $projectionLifeCycle,
                    $projectionAttribute->getEventStoreReferenceName()
                );
            } else if ($projectionAttribute->getFromStreams()) {
                $projectionConfiguration = ProjectionSetupConfiguration::fromStreams(
                    $projectionAttribute->getName(),
                    $projectionLifeCycle,
                    $projectionAttribute->getEventStoreReferenceName(),
                    ...$projectionAttribute->getFromStreams()
                );
            } else {
                $projectionConfiguration = ProjectionSetupConfiguration::fromCategories(
                    $projectionAttribute->getName(),
                    $projectionLifeCycle,
                    $projectionAttribute->getEventStoreReferenceName(),
                    ...$projectionAttribute->getFromCategories()
                );
            }

            $projectionSetupConfigurations[$projectionAttribute->getName()] = $projectionConfiguration;
        }

        foreach ($projectionEventHandlers as $projectionEventHandler) {
            /** @var Projection $projectionAttribute */
            $projectionAttribute     = $projectionEventHandler->getAnnotationForClass();
            /** @var EndpointAnnotation $handlerAttribute */
            $handlerAttribute     = $projectionEventHandler->getAnnotationForMethod();
            $projectionConfiguration = $projectionSetupConfigurations[$projectionAttribute->getName()];

            $eventHandlerChannelName = ModellingHandlerModule::getHandlerChannel($projectionEventHandler);
            $synchronousEventHandlerRequestChannel = AsynchronousModule::create($annotationRegistrationService)->getSynchronousChannelFor($eventHandlerChannelName, $handlerAttribute->getEndpointId());
            $projectionSetupConfigurations[$projectionAttribute->getName()] = $projectionConfiguration->withProjectionEventHandler(
                ModellingHandlerModule::getNamedMessageChannelForEventHandler($projectionEventHandler),
                $projectionEventHandler->getClassName(),
                $projectionEventHandler->getMethodName(),
                $synchronousEventHandlerRequestChannel,
                $eventHandlerChannelName
            );
        }

        return new self($projectionSetupConfigurations, $projectionLifeCyclesServiceActivators, EventMapper::createWith($fromClassToNameMapping, $fromNameToClassMapping), AggregateStreamMapping::createWith($aggregateToStreamMapping), $relatedInterfaces, array_unique($requiredReferences));
    }

    public function prepare(Configuration $configuration, array $extensionObjects, ModuleReferenceSearchService $moduleReferenceSearchService): void
    {
        $moduleReferenceSearchService->store(EventMapper::class, $this->eventMapper);
        $moduleReferenceSearchService->store(AggregateStreamMapping::class, $this->aggregateToStreamMapping);
        $configuration->registerRelatedInterfaces($this->relatedInterfaces);
        $configuration->requireReferences($this->requiredReferences);

        $projectionRunningConfigurations = [];
        $eventSourcingConfigurations = [];
        foreach ($extensionObjects as $extensionObject) {
            if ($extensionObject instanceof EventSourcingConfiguration) {
                $eventSourcingConfigurations[] = $extensionObject;
            }else if ($extensionObject instanceof ProjectionRunningConfiguration) {
                $projectionRunningConfigurations[$extensionObject->getProjectionName()] = $extensionObject;
            }
        }

        if (!$eventSourcingConfigurations) {
            $eventSourcingConfigurations[] = EventSourcingConfiguration::createWithDefaults();
        }

        foreach ($eventSourcingConfigurations as $eventSourcingConfiguration) {
            foreach ($this->projectionSetupConfigurations as $index => $projectionSetupConfiguration) {
                if ($eventSourcingConfiguration->getEventStoreReferenceName() === $projectionSetupConfiguration->getEventStoreReferenceName())

                $generatedChannelName  = Uuid::uuid4()->toString();
                $projectionRunningConfiguration = ProjectionRunningConfiguration::createEventDriven($projectionSetupConfiguration->getProjectionName());
                if (array_key_exists($projectionSetupConfiguration->getProjectionName(), $projectionRunningConfigurations)) {
                    $projectionRunningConfiguration = $projectionRunningConfigurations[$projectionSetupConfiguration->getProjectionName()];
                }
                $projectionSetupConfiguration = $projectionSetupConfiguration
                                        ->withOptions([
                                            ReadModelProjector::OPTION_CACHE_SIZE => $projectionRunningConfiguration->getAmountOfCachedStreamNames(),
                                            ReadModelProjector::OPTION_SLEEP => $projectionRunningConfiguration->getWaitBeforeCallingESWhenNoEventsFound(),
                                            ReadModelProjector::OPTION_PERSIST_BLOCK_SIZE => $projectionRunningConfiguration->getPersistChangesAfterAmountOfOperations(),
                                            ReadModelProjector::OPTION_LOCK_TIMEOUT_MS => $projectionRunningConfiguration->getProjectionLockTimeout(),
                                            ReadModelProjector::DEFAULT_UPDATE_LOCK_THRESHOLD => $projectionRunningConfiguration->getUpdateLockTimeoutAfter()
                                        ]);

                $projectionExecutorBuilder = new ProjectionExecutorBuilder($eventSourcingConfiguration, $projectionSetupConfiguration, $this->projectionSetupConfigurations, $projectionRunningConfiguration, "execute");
                $projectionExecutorBuilder = $projectionExecutorBuilder->withInputChannelName($generatedChannelName);
                $configuration->registerMessageHandler($projectionExecutorBuilder);

                foreach ($projectionSetupConfiguration->getProjectionEventHandlers() as $projectionEventHandler) {
                    $configuration->registerBeforeSendInterceptor(MethodInterceptor::create(
                        Uuid::uuid4()->toString(),
                        InterfaceToCall::create(ProjectionFlowController::class, "preSend"),
                        ServiceActivatorBuilder::createWithDirectReference(new ProjectionFlowController($projectionRunningConfiguration->isPolling()), "preSend"),
                        Precedence::SYSTEM_PRECEDENCE_BEFORE,
                        $projectionEventHandler->getClassName() . "::" . $projectionEventHandler->getMethodName()
                    ));
                    $configuration->registerBeforeMethodInterceptor(MethodInterceptor::create(
                        Uuid::uuid4()->toString(),
                        InterfaceToCall::create(ProjectionExecutor::class, "beforeEventHandler"),
                        new ProjectionExecutorBuilder($eventSourcingConfiguration, $projectionSetupConfiguration, $this->projectionSetupConfigurations, $projectionRunningConfiguration, "beforeEventHandler"),
                        Precedence::SYSTEM_PRECEDENCE_BEFORE,
                        $projectionEventHandler->getClassName() . "::" . $projectionEventHandler->getMethodName()
                    ));
                }

                if ($projectionRunningConfiguration->isPolling()) {
                    $configuration->registerConsumer(
                        InboundChannelAdapterBuilder::createWithDirectObject(
                            $generatedChannelName,
                            new ProjectionChannelAdapter(),
                            "run"
                        )->withEndpointId($projectionSetupConfiguration->getProjectionName())
                    );
                }
            }
            foreach ($this->projectionLifeCycleServiceActivators as $serviceActivator) {
                $configuration->registerMessageHandler($serviceActivator);
            }

            $this->registerEventStore($configuration, $eventSourcingConfiguration);
            $this->registerProjectionManager($configuration, $eventSourcingConfiguration);
        }
    }

    public function canHandle($extensionObject): bool
    {
        return
            $extensionObject instanceof EventSourcingConfiguration
            ||
            $extensionObject instanceof ProjectionRunningConfiguration;
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

    private function registerEventStore(Configuration $configuration, EventSourcingConfiguration $eventSourcingConfiguration): void
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

    private function registerProjectionManager(Configuration $configuration, EventSourcingConfiguration $eventSourcingConfiguration): void
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
            self::ECOTONE_ES_DELETE_PROJECTION,
            [ConsoleCommandParameter::create("name", "ecotone.eventSourcing.manager.name", false), ConsoleCommandParameter::createWithDefaultValue("deleteEmittedEvents", "ecotone.eventSourcing.manager.deleteEmittedEvents", true, true)]
        );

        $this->registerProjectionManagerAction(
            "resetProjection",
            [HeaderBuilder::create("name", "ecotone.eventSourcing.manager.name")],
            [GatewayHeaderBuilder::create("name", "ecotone.eventSourcing.manager.name")],
            $eventSourcingConfiguration,
            $configuration,
            self::ECOTONE_ES_RESET_PROJECTION,
            [ConsoleCommandParameter::create("name", "ecotone.eventSourcing.manager.name", false)]
        );

        $this->registerProjectionManagerAction(
            "stopProjection",
            [HeaderBuilder::create("name", "ecotone.eventSourcing.manager.name")],
            [GatewayHeaderBuilder::create("name", "ecotone.eventSourcing.manager.name")],
            $eventSourcingConfiguration,
            $configuration,
            self::ECOTONE_ES_STOP_PROJECTION,
            [ConsoleCommandParameter::create("name", "ecotone.eventSourcing.manager.name", false)]
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

    private function registerProjectionManagerAction(string $methodName, array $endpointConverters, array $gatewayConverters, EventSourcingConfiguration $eventSourcingConfiguration, Configuration $configuration, ?string $consoleCommandName = null, array $consoleCommandParameters = []): void
    {
        $messageHandlerBuilder = ProjectionManagerBuilder::create($methodName, $endpointConverters, $eventSourcingConfiguration, $this->projectionSetupConfigurations);
        $configuration->registerMessageHandler($messageHandlerBuilder);
        $configuration->registerGatewayBuilder(
            GatewayProxyBuilder::create($eventSourcingConfiguration->getProjectManagerReferenceName(), ProjectionManager::class, $methodName, $messageHandlerBuilder->getInputMessageChannelName())
                ->withParameterConverters($gatewayConverters)
        );

        if ($consoleCommandName) {
            $configuration->registerConsoleCommand(
                ConsoleCommandConfiguration::create(
                    $messageHandlerBuilder->getInputMessageChannelName(),
                    $consoleCommandName,
                    $consoleCommandParameters
                )
            );
        }
    }

    private function registerEventStoreAction(string $methodName, array $endpointConverters, array $gatewayConverters, EventSourcingConfiguration $eventSourcingConfiguration, Configuration $configuration): void
    {
        $messageHandlerBuilder = EventStoreBuilder::create($methodName, $endpointConverters, $eventSourcingConfiguration);
        $configuration->registerMessageHandler($messageHandlerBuilder);

        $configuration->registerGatewayBuilder(
            GatewayProxyBuilder::create($eventSourcingConfiguration->getEventStoreReferenceName(), EventStore::class, $methodName, $messageHandlerBuilder->getInputMessageChannelName())
                ->withParameterConverters($gatewayConverters)
        );
    }
}