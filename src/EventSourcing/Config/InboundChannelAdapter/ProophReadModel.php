<?php


namespace Ecotone\EventSourcing\Config\InboundChannelAdapter;


use Ecotone\Messaging\Gateway\MessagingEntrypoint;
use Prooph\EventStore\Projection\ReadModel;

class ProophReadModel implements ReadModel
{
    private MessagingEntrypoint $messagingEntrypoint;
    private ?string $initializationRequestChannel;
    private ?string $resetRequestChannel;
    private ?string $deleteRequestChannel;

    public function __construct(MessagingEntrypoint $messagingEntrypoint, ?string $initializationRequestChannel, ?string $resetRequestChannel, ?string $deleteRequestChannel)
    {
        $this->initializationRequestChannel = $initializationRequestChannel;
        $this->resetRequestChannel = $resetRequestChannel;
        $this->deleteRequestChannel = $deleteRequestChannel;
        $this->messagingEntrypoint = $messagingEntrypoint;
    }

    public function init(): void
    {
        if (!$this->initializationRequestChannel) {
            return;
        }

        $this->messagingEntrypoint->send([], $this->initializationRequestChannel);
    }

    public function isInitialized(): bool
    {
        return false;
    }

    public function reset(): void
    {
        if (!$this->resetRequestChannel) {
            return;
        }

        $this->messagingEntrypoint->send([], $this->resetRequestChannel);
    }

    public function delete(): void
    {
        if (!$this->deleteRequestChannel) {
            return;
        }

        $this->messagingEntrypoint->send([], $this->deleteRequestChannel);
    }

    public function stack(string $operation, ...$args): void
    {
        return;
    }

    public function persist(): void
    {
        return;
    }
}