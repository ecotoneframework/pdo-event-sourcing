<?php


namespace Ecotone\EventSourcing\Config\InboundChannelAdapter;


use Ecotone\EventSourcing\LazyProophProjectionManager;
use Ecotone\EventSourcing\ProjectionConfiguration;
use Ecotone\Messaging\Gateway\MessagingEntrypoint;
use Prooph\Common\Messaging\Message;

class ProjectionExecutor
{
    private LazyProophProjectionManager $lazyProophProjectionManager;
    private ProjectionConfiguration $projectionConfiguration;

    public function __construct(LazyProophProjectionManager $lazyProophProjectionManager, ProjectionConfiguration $projectionConfiguration)
    {
        $this->lazyProophProjectionManager = $lazyProophProjectionManager;
        $this->projectionConfiguration = $projectionConfiguration;
    }

    public function execute(MessagingEntrypoint $messagingEntrypoint) : void
    {
        if ($this->projectionConfiguration->getProjectionLifeCycleConfiguration()->getInitializationRequestChannel()) {
            $messagingEntrypoint->send([], $this->projectionConfiguration->getProjectionLifeCycleConfiguration()->getInitializationRequestChannel());
        }

        $handlers = [];
        foreach ($this->projectionConfiguration->getProjectionEventHandlerChannels() as $eventName => $targetChannel) {
            $projectionConfiguration = $this->projectionConfiguration;
            $handlers[$eventName] = function ($state, Message $event) use ($messagingEntrypoint, $targetChannel, $projectionConfiguration) : mixed {
                $result = $messagingEntrypoint->sendWithHeaders(
                    $event->payload(),
                    array_merge($event->metadata(), ["projection.state" => $state, "projection.name" => $projectionConfiguration->getProjectionName()]),
                    $targetChannel
                );

                return $projectionConfiguration->isKeepingStateBetweenEvents() ? $result : null;
            };
        }

        $readModel = new ProophReadModel($messagingEntrypoint, $this->projectionConfiguration->getProjectionLifeCycleConfiguration()->getInitializationRequestChannel(), $this->projectionConfiguration->getProjectionLifeCycleConfiguration()->getResetRequestChannel(), $this->projectionConfiguration->getProjectionLifeCycleConfiguration()->getDeleteRequestChannel());
        $projection = $this->lazyProophProjectionManager->createReadModelProjection($this->projectionConfiguration->getProjectionName(), $readModel, $this->projectionConfiguration->getProjectionOptions());
        if  ($this->projectionConfiguration->isWithAllStreams()) {
            $projection = $projection->fromAll();
        }else if ($this->projectionConfiguration->getCategories()) {
            $projection = $projection->fromCategories(...$this->projectionConfiguration->getCategories());
        }else if ($this->projectionConfiguration->getStreamNames()) {
            $projection = $projection->fromStreams(...$this->projectionConfiguration->getStreamNames());
        }
        $projection = $projection->when($handlers);

        $projection->run(false);
    }
}