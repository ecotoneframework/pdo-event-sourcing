<?php


namespace Ecotone\EventSourcing\Config;

use Ecotone\AnnotationFinder\AnnotationFinder;
use Ecotone\EventSourcing\EventMapper;
use Ecotone\Messaging\Attribute\ModuleAnnotation;
use Ecotone\Messaging\Config\Annotation\ModuleConfiguration\NoExternalConfigurationModule;
use Ecotone\Messaging\Config\Configuration;
use Ecotone\Messaging\Config\ModuleReferenceSearchService;

#[ModuleAnnotation]
class EventSourcingModule extends NoExternalConfigurationModule
{
    public static function create(AnnotationFinder $annotationRegistrationService): static
    {

    }

    public function prepare(Configuration $configuration, array $extensionObjects, ModuleReferenceSearchService $moduleReferenceSearchService): void
    {
        $moduleReferenceSearchService->store(EventMapper::class, new EventMapper([], []));
    }

    public function canHandle($extensionObject): bool
    {
        return false;
    }
}