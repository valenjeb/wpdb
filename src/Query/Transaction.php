<?php

declare(strict_types=1);

namespace Devly\WPDB\Query;

use Closure;
use Devly\WPDB\Exceptions\QueryBuilderException;
use Throwable;

class Transaction extends QueryBuilder
{
    protected ?string $transactionStatement;

    /** @param Closure(self): void $callback */
    public function transaction(Closure $callback): Transaction
    {
        $callback($this);

        return $this;
    }

    /**
     * Commit transaction
     */
    public function commit(): void
    {
        try {
            $this->db()->commit();
        } catch (Throwable $e) {
            throw new QueryBuilderException($e->getMessage(), $e->getCode(), $e, $this->getQuery());
        }
    }

    /**
     * Rollback transaction
     */
    public function rollBack(): void
    {
        try {
            $this->db()->rollBack();
        } catch (Throwable $e) {
            throw new QueryBuilderException($e->getMessage(), $e->getCode(), $e, $this->getQuery());
        }
    }

    /**
     * Execute statement
     *
     * @param string                  $sql      SQL query
     * @param array<string|int|float> $bindings
     */
    public function statement(string $sql, array $bindings = []): string
    {
        if ($this->transactionStatement === null && $this->db()->inTransaction() === true) {
            $results                    = parent::statement($sql, $bindings);
            $this->transactionStatement = $results;

            return $results;
        }

        return parent::statement($sql, $bindings);
    }
}
