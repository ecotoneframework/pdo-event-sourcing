<?php


namespace Ecotone\EventSourcing\Config;

use Ecotone\AnnotationFinder\AnnotationFinder;
use Ecotone\EventSourcing\Attribute\Projection;
use Ecotone\EventSourcing\EventMapper;
use Ecotone\EventSourcing\EventSourcingConfiguration;
use Ecotone\EventSourcing\LazyProophEventStore;
use Ecotone\EventSourcing\ProjectionConfiguration;
use Ecotone\EventSourcing\ProjectionLifeCycleConfiguration;
use Ecotone\EventSourcing\ProophRepositoryBuilder;
use Ecotone\Messaging\Attribute\ModuleAnnotation;
use Ecotone\Messaging\Config\Annotation\ModuleConfiguration\NoExternalConfigurationModule;
use Ecotone\Messaging\Config\Configuration;
use Ecotone\Messaging\Config\ModuleReferenceSearchService;
use Ecotone\Messaging\Endpoint\InboundChannelAdapter\InboundChannelAdapterBuilder;
use Ecotone\Messaging\Support\Assert;
use Ecotone\Modelling\Attribute\EventHandler;
use Ecotone\Modelling\Config\ModellingHandlerModule;

#[ModuleAnnotation]
class EventSourcingModule extends NoExternalConfigurationModule
{
    /**
     * @var ProjectionConfiguration[]
     */
    private array $projectionConfigurations;

    /**
     * @var ProjectionConfiguration[]
     */
    public function __construct(array $projectionConfigurations)
    {
        $this->projectionConfigurations = $projectionConfigurations;
    }

    public static function create(AnnotationFinder $annotationRegistrationService): static
    {
        $projectionClassNames = $annotationRegistrationService->findAnnotatedClasses(Projection::class);
        $projectionEventHandlers = $annotationRegistrationService->findCombined(Projection::class, EventHandler::class);

        $projectionConfigurations = [];
        foreach ($projectionClassNames as $projectionClassName) {
            $attributes = $annotationRegistrationService->getAnnotationsForClass($projectionClassName);
            $projectionAttribute = null;
            foreach ($attributes as $attribute) {
                if ($attribute instanceof Projection) {
                    $projectionAttribute = $attribute;
                    break;
                }
            }

            Assert::keyNotExists($projectionAttribute->getName(), $projectionConfigurations, "Can't define projection with name {$projectionAttribute->getName()} twice");

            $projectionLifeCycle = ProjectionLifeCycleConfiguration::create();
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
                                            ->withOptions($projectionAttribute->getProjectionOptions());
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

        return new self($projectionConfigurations);
    }

    public function prepare(Configuration $configuration, array $extensionObjects, ModuleReferenceSearchService $moduleReferenceSearchService): void
    {
        $moduleReferenceSearchService->store(EventMapper::class, EventMapper::createEmpty());
        $eventStoreConfiguration = EventSourcingConfiguration::createWithDefaults();
        foreach ($extensionObjects as $extensionObject) {
            if ($extensionObject instanceof EventSourcingConfiguration) {
                $eventStoreConfiguration = $extensionObject;
            }
        }

        $lazyProophProjectionManager = null;

        foreach ($this->projectionConfigurations as $projectionConfiguration) {
            $projectionConfiguration->
        }

        $configuration->registerConsumer(InboundChannelAdapterBuilder::createWithDirectObject(

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

        return [ProophRepositoryBuilder::create()];
    }
}