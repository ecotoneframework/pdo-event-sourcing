<?php


namespace Ecotone\EventSourcing;

use Prooph\EventStore\Metadata\MetadataMatcher;

interface EventStore
{
    public function updateStreamMetadata(string $streamName, array $newMetadata): void;

    /**
     * @param Event[]|object[]|array[] $streamStreamEvents
     * @param string[] $streamMetadata
     */
    public function create(string $streamName, array $streamStreamEvents, array $streamMetadata): void;
    /**
     * @param Event[]|object[]|array[] $streamStreamEvents
     */
    public function appendTo(string $streamName, array $streamStreamEvents): void;

    public function delete(string $streamName): void;

    public function fetchStreamMetadata(string $streamName): array;

    public function hasStream(string $streamName): bool;

    /**
     * @return Event[]
     */
    public function load(
        string $streamName,
        int $fromNumber = 1,
        int $count = null,
        MetadataMatcher $metadataMatcher = null,
        bool $deserialize = true
    ): iterable;

    /**
     * @return Event[]
     */
    public function loadReverse(
        string $streamName,
        int $fromNumber = null,
        int $count = null,
        MetadataMatcher $metadataMatcher = null,
        bool $deserialize = true
    ): iterable;

    /**
     * @return string[]
     */
    public function fetchStreamNames(
        ?string $filter,
        ?MetadataMatcher $metadataMatcher,
        int $limit = 20,
        int $offset = 0
    ): array;

    /**
     * @return string[]
     */
    public function fetchStreamNamesRegex(
        string $filter,
        ?MetadataMatcher $metadataMatcher,
        int $limit = 20,
        int $offset = 0
    ): array;

    /**
     * @return string[]
     */
    public function fetchCategoryNames(?string $filter, int $limit = 20, int $offset = 0): array;

    /**
     * @return string[]
     */
    public function fetchCategoryNamesRegex(string $filter, int $limit = 20, int $offset = 0): array;
}