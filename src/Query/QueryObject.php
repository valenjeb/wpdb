<?php

declare(strict_types=1);

namespace Devly\WPDB\Query;

use Devly\WPDB\Connection;

use function is_array;
use function is_float;
use function is_int;
use function is_object;
use function is_string;
use function preg_replace;

class QueryObject
{
    protected string $sql;

    /** @var array */
    protected array $bindings = [];

    protected Connection $connection;

    /** @param array $bindings */
    public function __construct(string $sql, array $bindings, Connection $connection)
    {
        $this->sql        = $sql;
        $this->bindings   = $bindings;
        $this->connection = $connection;
    }

    /** @return array */
    public function getBindings(): array
    {
        return $this->bindings;
    }

    /**
     * Get the raw/bound sql
     */
    public function getRawSql(): string
    {
        return $this->interpolateQuery($this->sql, $this->bindings);
    }

    public function getSql(): string
    {
        return $this->sql;
    }

    /**
     * Replaces any parameter placeholders in a query with the value of that
     * parameter. Useful for debugging. Assumes anonymous parameters from
     * $params are in the same order as specified in $query
     *
     * Reference: http://stackoverflow.com/a/1376838/656489
     *
     * @param string $query  The sql query with parameter placeholders
     * @param array  $params The array of substitution parameters
     *
     * @return string The interpolated query
     */
    protected function interpolateQuery(string $query, array $params): string
    {
        $keys   = [];
        $values = $params;
        $db     = $this->getConnection()->getDatabaseConnection();

        // build a regular expression for each parameter
        foreach ($params as $key => $value) {
            $keys[] = '/' . (is_string($key) ? ':' . $key : '[?]') . '/';

            if ($value instanceof Raw) {
                continue;
            }

            // Try to parse object-types
            if (is_object($value) === true) {
                $value = (string) $value;
            }

            if (is_string($value) === true) {
                $values[$key] = $db->prepare('%s', $value);

                continue;
            }

            if (is_float($value) === true) {
                $values[$key] = $db->prepare('%f', $value);
                continue;
            }

            if (is_int($value) === true) {
                $values[$key] = $db->prepare('%d', $value);
                continue;
            }

            if (is_array($value) === true) {
                $values[$key] = $value;
                continue;
            }

            if ($value !== null) {
                continue;
            }

            $values[$key] = 'NULL';
        }

        return preg_replace($keys, $values, $query, 1, $count);
    }

    public function getConnection(): Connection
    {
        return $this->connection;
    }
}
