<?php

declare(strict_types=1);

namespace Devly\WPDB;

use Devly\WPDB\Query\QueryBuilder;
use Devly\WPDB\Query\QueryObject;

use function trim;

class Event
{
    /**
     * Event name
     */
    private string $name;

    private QueryObject $queryObject;

    private QueryBuilder $queryBuilder;

    /** @var array<string, mixed> */
    private array $arguments;

    /** @param array<string, mixed> $arguments */
    public function __construct(
        string $name,
        QueryObject $queryObject,
        QueryBuilder $queryBuilder,
        array $arguments = []
    ) {
        $this->name         = $name;
        $this->queryObject  = $queryObject;
        $this->queryBuilder = $queryBuilder;
        $this->arguments    = $arguments;
    }

    /**
     * Get event name
     */
    public function getEventName(): string
    {
        return $this->name;
    }

    /**
     * Get QueryBuilder object
     */
    public function getQueryBuilder(): QueryBuilder
    {
        return $this->queryBuilder;
    }

    /**
     * Get query object
     */
    public function getQuery(): QueryObject
    {
        return $this->queryObject;
    }

    /**
     * Get insert id from last query
     */
    public function getInsertId(): ?string
    {
        return $this->arguments['insert_id'] ?? null;
    }

    /**
     * Get execution time
     */
    public function getExecutionTime(): ?float
    {
        return $this->getArguments('execution_time');
    }

    /**
     * Get arguments
     *
     * @return array<string, mixed>|mixed
     */
    public function getArguments(?string $key = null)
    {
        if ($key === null || empty(trim($key))) {
            return $this->arguments;
        }

        return $this->arguments[$key] ?? null;
    }

    public function getConnection(): Connection
    {
        return $this->getQueryBuilder()->getConnection();
    }
}
