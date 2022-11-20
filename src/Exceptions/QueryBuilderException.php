<?php

declare(strict_types=1);

namespace Devly\WPDB\Exceptions;

use Devly\Exceptions\DatabaseException;
use Devly\WPDB\Query\QueryObject;
use Throwable;

class QueryBuilderException extends DatabaseException
{
    protected ?QueryObject $query;

    public function __construct(string $message = '', int $code = 0, ?Throwable $previous = null, ?QueryObject $query = null)
    {
        parent::__construct($message, $code, $previous);
        $this->query = $query;
    }

    /**
     * Get query-object from last executed query.
     */
    public function getQuery(): ?QueryObject
    {
        return $this->query;
    }
}
