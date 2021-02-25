<?php


namespace Ecotone\EventSourcing\Config;

use Ecotone\AnnotationFinder\AnnotationFinder;
use Ecotone\EventSourcing\EventMapper;
use Ecotone\EventSourcing\ProophRepositoryBuilder;
use Ecotone\Messaging\Attribute\ModuleAnnotation;
use Ecotone\Messaging\Config\Annotation\ModuleConfiguration\NoExternalConfigurationModule;
use Ecotone\Messaging\Config\Configuration;
use Ecotone\Messaging\Config\ModuleReferenceSearchService;

#[ModuleAnnotation]
class EventSourcingModule extends NoExternalConfigurationModule
{
    public static function create(AnnotationFinder $annotationRegistrationService): static
    {
        return new self();
    }

    public function prepare(Configuration $configuration, array $extensionObjects, ModuleReferenceSearchService $moduleReferenceSearchService): void
    {
        $moduleReferenceSearchService->store(EventMapper::class, EventMapper::createEmpty());


    }

    public function canHandle($extensionObject): bool
    {
        return false;
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