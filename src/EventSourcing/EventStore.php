<?php


namespace Ecotone\EventSourcing;


use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;

interface EventStore
{
    public function updateStreamMetadata(string $streamName, array $newMetadata): void;

    /**
     * @param EventWithMetadata[]|object[]|array[] $streamEvents
     * @param string[] $streamMetadata
     */
    public function create(string $streamName, array $streamEvents, array $streamMetadata): void;
    /**
     * @param EventWithMetadata[]|object[]|array[] $streamEvents
     */
    public function appendTo(string $streamName, array $streamEvents): void;

    public function delete(string $streamName): void;

    public function fetchStreamMetadata(string $streamName): array;

    public function hasStream(string $streamName): bool;

    /**
     * @return EventWithMetadata[]
     */
    public function load(
        string $streamName,
        int $fromNumber = 1,
        int $count = null,
        MetadataMatcher $metadataMatcher = null
    ): iterable;

    /**
     * @return EventWithMetadata[]
     */
    public function loadReverse(
        string $streamName,
        int $fromNumber = null,
        int $count = null,
        MetadataMatcher $metadataMatcher = null
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