<?php

declare(strict_types=1);

namespace Devly\WPDB\Contracts;

use Closure;
use Devly\WPDB\Query\Raw;

interface IQueryAdapter
{
    /**
     * @param array<string, mixed> $statements
     *
     * @return array{sql: string, bindings: array<string|int|float>}
     */
    public function select(array $statements): array;

    /**
     * Build insert query
     *
     * @param array<string, mixed> $statements
     * @param array<string, mixed> $data
     *
     * @return array{sql: string, bindings: array<string|int|float>|mixed}
     */
    public function insert(array $statements, array $data): array;

    /**
     * Build insert and ignore query
     *
     * @param array<string, mixed> $statements
     * @param array<string, mixed> $data
     *
     * @return array{sql: string, bindings: array<string|int|float>}
     */
    public function insertIgnore(array $statements, array $data): array;

    /**
     * Build update query
     *
     * @param array<string, mixed> $statements
     * @param array<string, mixed> $data
     *
     * @return array{sql: string, bindings: array<string|int|float>}
     */
    public function update(array $statements, array $data): array;

    /**
     * Build replace query
     *
     * @param array<string, mixed> $statements
     * @param string[]             $data
     *
     * @return array{sql: string, bindings: array<string|int|float>}
     */
    public function replace(array $statements, array $data): array;

    /**
     * Build delete query
     *
     * @param array<string, mixed> $statements
     * @param string[]|null        $columns
     *
     * @return array{sql: string, bindings: array<string|int|float>}
     */
    public function delete(array $statements, ?array $columns = null): array;

    /**
     * Build just criteria part of the query
     *
     * @param array<string, mixed> $statements
     *
     * @return array{sql: string|string[], bindings: array<string|int|float>}
     */
    public function criteriaOnly(array $statements, bool $bindValues = true): array;

    /**
     * Wrap values with adapter's sanitizer like, '`'
     *
     * @param string|Raw|Closure $value
     *
     * @return string|Closure
     */
    public function wrapSanitizer($value);
}
