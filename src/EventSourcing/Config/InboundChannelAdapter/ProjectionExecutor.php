<?php


namespace Ecotone\EventSourcing\Config\InboundChannelAdapter;


use Ecotone\EventSourcing\LazyProophProjectionManager;
use Ecotone\EventSourcing\ProjectionRunningConfiguration;
use Ecotone\EventSourcing\ProjectionSetupConfiguration;
use Ecotone\Messaging\Config\MessagingSystemConfiguration;
use Ecotone\Messaging\Gateway\MessagingEntrypoint;
use Ecotone\Messaging\Gateway\MessagingEntrypointWithHeadersPropagation;
use Ecotone\Messaging\NullableMessageChannel;
use Prooph\Common\Messaging\Message;
use Prooph\EventStore\Projection\ProjectionStatus;
use Prooph\EventStore\StreamName;

class ProjectionExecutor
{
    const PROJECTION_STATE            = "projection.state";
    const PROJECTION_IS_RESETTING            = "projection.is_resetting";
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

    public function beforeEventHandler(\Ecotone\Messaging\Message $message, MessagingEntrypointWithHeadersPropagation $messagingEntrypoint) : ?\Ecotone\Messaging\Message
    {
        if ($this->shouldBePassedToEventHandler($message)) {
            return $message;
        }

        $this->execute($messagingEntrypoint);

        return null;
    }

    public function execute(MessagingEntrypointWithHeadersPropagation $messagingEntrypoint) : void
    {
        if (!$this->wasInitialized && $this->projectionConfiguration->getProjectionLifeCycleConfiguration()->getInitializationRequestChannel()) {
            $messagingEntrypoint->send([], $this->projectionConfiguration->getProjectionLifeCycleConfiguration()->getInitializationRequestChannel());
            $this->wasInitialized = true;
        }

        $readModel = new ProophReadModel(
            $messagingEntrypoint,
            $this->projectionConfiguration->getProjectionLifeCycleConfiguration(),
            $this->projectionRunningConfiguration
        );
        $projection = $this->lazyProophProjectionManager->createReadModelProjection($this->projectionConfiguration->getProjectionName(), $readModel, $this->projectionConfiguration->getProjectionOptions());


        $status = ProjectionStatus::RUNNING;
        $projectHasRelatedStream = $this->lazyProophProjectionManager->fetchProjectionNames($projection->getName());
        if ($projectHasRelatedStream) {
            $status = $this->lazyProophProjectionManager->fetchProjectionStatus($projection->getName())->getValue();
        }

        $handlers = [];
        $projectionEventHandlers    = $this->projectionConfiguration->getProjectionEventHandlers();
        foreach ($projectionEventHandlers as $eventName => $projectionEventHandler) {
            $projectionConfiguration = $this->projectionConfiguration;
            $handlers[$eventName] = function ($state, Message $event) use ($messagingEntrypoint, $projectionEventHandler, $projectionConfiguration, $status) : mixed {
                $result = $messagingEntrypoint->sendWithHeaders(
                    $event->payload(),
                    array_merge($event->metadata(), [
                            self::PROJECTION_STATE => $state,
                            self::PROJECTION_IS_RESETTING => $status === ProjectionStatus::RESETTING,
                            self::PROJECTION_NAME => $projectionConfiguration->getProjectionName(),
                            self::PROJECTION_IS_POLLING => true
                        ]
                    ),
                    $projectionEventHandler->getSynchronousRequestChannelName()
                );

                return $projectionConfiguration->isKeepingStateBetweenEvents() ? $result : null;
            };
        }

        if  ($this->projectionConfiguration->isWithAllStreams()) {
            $projection = $projection->fromAll();
        }else if ($this->projectionConfiguration->getCategories()) {
            $projection = $projection->fromCategories(...$this->projectionConfiguration->getCategories());
        }else if ($this->projectionConfiguration->getStreamNames()) {
            $projection = $projection->fromStreams(...$this->projectionConfiguration->getStreamNames());
        }
        $projection = $projection->when($handlers);

        if ($this->projectionRunningConfiguration->isTestingSetup()) {
            usleep(40);
        }
        $projection->run(false);

        if ($status === ProjectionStatus::DELETING_INCL_EMITTED_EVENTS && $projectHasRelatedStream) {
            $projectionStreamName = new StreamName(LazyProophProjectionManager::getProjectionStreamName($projection->getName()));
            if ($this->lazyProophProjectionManager->getLazyProophEventStore()->hasStream($projectionStreamName)) {
                $this->lazyProophProjectionManager->getLazyProophEventStore()->delete($projectionStreamName);
            }
        }
    }

    private function shouldBePassedToEventHandler(\Ecotone\Messaging\Message $message)
    {
        return $message->getHeaders()->containsKey(ProjectionExecutor::PROJECTION_IS_POLLING)
            ? $message->getHeaders()->get(ProjectionExecutor::PROJECTION_IS_POLLING)
            : false;
    }
}