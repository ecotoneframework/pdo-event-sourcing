<?php


namespace Ecotone\EventSourcing\Config\InboundChannelAdapter;


use Ecotone\EventSourcing\LazyProophProjectionManager;
use Ecotone\EventSourcing\ProjectionRunningConfiguration;
use Ecotone\EventSourcing\ProjectionSetupConfiguration;
use Ecotone\Messaging\Config\MessagingSystemConfiguration;
use Ecotone\Messaging\Gateway\MessagingEntrypoint;
use Ecotone\Messaging\NullableMessageChannel;
use Prooph\Common\Messaging\Message;

class ProjectionExecutor
{
    const PROJECTION_STATE            = "projection.state";
    const PROJECTION_NAME             = "projection.name";
    const PROJECTION_IS_POLLING = "projection.isPolling";

    private LazyProophProjectionManager $lazyProophProjectionManager;
    private ProjectionSetupConfiguration $projectionConfiguration;
    private bool $wasInitialized = false;
    private ProjectionRunningConfiguration $projectionRunningConfiguration;

    public function __construct(LazyProophProjectionManager $lazyProophProjectionManager, ProjectionSetupConfiguration $projectionConfiguration, ProjectionRunningConfiguration $projectionRunningConfiguration)
    {
        $this->lazyProophProjectionManager = $lazyProophProjectionManager;
        $this->projectionConfiguration = $projectionConfiguration;
        $this->projectionRunningConfiguration = $projectionRunningConfiguration;
    }

    public function beforeEventHandler(\Ecotone\Messaging\Message $message, MessagingEntrypoint $messagingEntrypoint) : ?\Ecotone\Messaging\Message
    {
        if ($this->shouldBePassedToEventHandler($message)) {
            return $message;
        }

        $this->execute($messagingEntrypoint);

        return null;
    }

    public function execute(MessagingEntrypoint $messagingEntrypoint) : void
    {
        if (!$this->wasInitialized && $this->projectionConfiguration->getProjectionLifeCycleConfiguration()->getInitializationRequestChannel()) {
            $messagingEntrypoint->send([], $this->projectionConfiguration->getProjectionLifeCycleConfiguration()->getInitializationRequestChannel());
            $this->wasInitialized = true;
        }

        $handlers = [];
        $projectionEventHandlers    = $this->projectionConfiguration->getProjectionEventHandlers();
        foreach ($projectionEventHandlers as $eventName => $projectionEventHandler) {
            $projectionConfiguration = $this->projectionConfiguration;
            $handlers[$eventName] = function ($state, Message $event) use ($messagingEntrypoint, $projectionEventHandler, $projectionConfiguration) : mixed {
                $result = $messagingEntrypoint->sendWithHeaders(
                    $event->payload(),
                    array_merge($event->metadata(), [
                            self::PROJECTION_STATE => $state,
                            self::PROJECTION_NAME => $projectionConfiguration->getProjectionName(),
                            self::PROJECTION_IS_POLLING => true
                        ]
                    ),
                    $projectionEventHandler->getSynchronousRequestChannelName()
                );

                return $projectionConfiguration->isKeepingStateBetweenEvents() ? $result : null;
            };
        }

        $readModel = new ProophReadModel(
            $messagingEntrypoint,
            $this->projectionConfiguration->getProjectionLifeCycleConfiguration(),
            $this->projectionRunningConfiguration
        );
        $projection = $this->lazyProophProjectionManager->createReadModelProjection($this->projectionConfiguration->getProjectionName(), $readModel, $this->projectionConfiguration->getProjectionOptions());
        if  ($this->projectionConfiguration->isWithAllStreams()) {
            $projection = $projection->fromAll();
        }else if ($this->projectionConfiguration->getCategories()) {
            $projection = $projection->fromCategories(...$this->projectionConfiguration->getCategories());
        }else if ($this->projectionConfiguration->getStreamNames()) {
            $projection = $projection->fromStreams(...$this->projectionConfiguration->getStreamNames());
        }
        $projection = $projection->when($handlers);

        $this->lazyProophProjectionManager->ensureEventStoreIsPrepared();

        if ($this->projectionRunningConfiguration->isTestingSetup()) {
            usleep(10);
        }
        $projection->run(false);
    }

    private function shouldBePassedToEventHandler(\Ecotone\Messaging\Message $message)
    {
        return $message->getHeaders()->containsKey(ProjectionExecutor::PROJECTION_IS_POLLING)
            ? $message->getHeaders()->get(ProjectionExecutor::PROJECTION_IS_POLLING)
            : false;
    }
}