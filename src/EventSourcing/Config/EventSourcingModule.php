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
use Ecotone\EventSourcing\ProjectionConfiguration;
use Ecotone\EventSourcing\ProjectionLifeCycleConfiguration;
use Ecotone\EventSourcing\ProophRepositoryBuilder;
use Ecotone\Messaging\Attribute\ModuleAnnotation;
use Ecotone\Messaging\Config\Annotation\AnnotatedDefinitionReference;
use Ecotone\Messaging\Config\Annotation\ModuleConfiguration\NoExternalConfigurationModule;
use Ecotone\Messaging\Config\Configuration;
use Ecotone\Messaging\Config\ConsoleCommandConfiguration;
use Ecotone\Messaging\Config\ModuleReferenceSearchService;
use Ecotone\Messaging\Endpoint\InboundChannelAdapter\InboundChannelAdapterBuilder;
use Ecotone\Messaging\Handler\ClassDefinition;
use Ecotone\Messaging\Handler\ServiceActivator\ServiceActivatorBuilder;
use Ecotone\Messaging\Handler\TypeDescriptor;
use Ecotone\Messaging\Support\Assert;
use Ecotone\Modelling\Attribute\EventHandler;
use Ecotone\Modelling\Config\ModellingHandlerModule;
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
        $configuration->registerConsoleCommand(ConsoleCommandConfiguration::create(
            "test",
            "test",
            []
        ));
    }

    public function canHandle($extensionObject): bool
    {
        return $extensionObject instanceof EventSourcingConfiguration;
    }

    public function getModuleExtensions(array $serviceExtensions): array
    {
        foreach ($serviceExtensions as $serviceExtension) {
            if ($serviceExtension instanceof ProophRepositoryBuilder) {
                return [];
            }
        }

        return [ProophRepositoryBuilder::create(EventSourcingConfiguration::createWithDefaults())];
    }
}