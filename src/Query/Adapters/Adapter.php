<?php

declare(strict_types=1);

namespace Devly\WPDB\Query\Adapters;

use Closure;
use Devly\Exceptions\DatabaseException;
use Devly\WPDB\Connection;
use Devly\WPDB\Contracts\IQueryAdapter;
use Devly\WPDB\Query\NestedCriteria;
use Devly\WPDB\Query\QueryBuilder;
use Devly\WPDB\Query\Raw;

use function array_merge;
use function array_values;
use function assert;
use function compact;
use function count;
use function end;
use function explode;
use function implode;
use function is_array;
use function is_int;
use function is_object;
use function is_string;
use function method_exists;
use function sprintf;
use function str_ireplace;
use function strpos;
use function strtolower;
use function strtoupper;
use function trim;

abstract class Adapter implements IQueryAdapter
{
    public const SANITIZER = '';

    protected const QUERY_PART_JOIN = 'JOIN';

    protected const QUERY_PART_ORDERBY = 'ORDERBY';

    protected const QUERY_PART_LIMIT = 'LIMIT';

    protected const QUERY_PART_OFFSET = 'OFFSET';

    protected const QUERY_PART_FOR = 'FOR';

    protected const QUERY_PART_GROUPBY = 'GROUPBY';

    protected const QUERY_PART_TOP = 'TOP';

    protected Connection $connection;

    protected ?string $aliasPrefix;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    /**
     * Join different part of queries with a space.
     *
     * @param string[] $pieces
     *
     * @return string Concatenate query string
     */
    protected function concatenateQuery(array $pieces): string
    {
        $str = '';
        foreach ($pieces as $piece) {
            $str = trim($str) . ' ' . trim($piece);
        }

        return trim($str);
    }

    /**
     * Array concatenating method, like implode.
     * But it does wrap sanitizer and trims last glue
     *
     * @param string[] $pieces
     */
    protected function arrayStr(array $pieces, string $glue = ', ', bool $wrapSanitizer = true): string
    {
        $str = '';
        foreach ($pieces as $key => $piece) {
            if (strpos($piece, ' as ') !== false) {
                [$key, $piece] = $this->splitAliasStatement($piece);
            }

            if ($wrapSanitizer === true) {
                $piece = $this->wrapSanitizer($piece);
            }

            if (is_int($key) === false) {
                $piece = ($wrapSanitizer && strpos($key, ')') === false
                        ? $this->wrapSanitizer($key)
                        : $key) . ' AS ' . $piece;
            }

            $str .= $piece . $glue;
        }

        return trim($str, $glue);
    }

    /**
     * Build generic criteria string and bindings from statements, like "a = b and c = ?"
     *
     * @param array<string|mixed> $statements
     *
     * @return array{string, string[]|int[]|float[]}
     */
    protected function buildCriteria(array $statements, bool $bindValues = true): array
    {
        $criteria = [];
        $bindings = [[]];

        foreach (array_values($statements) as $i => $statement) {
            if ($i === 0 && isset($statement['condition'])) {
                $criteria[] = $statement['condition'];
            }

            $joiner = $i === 0 ? trim(str_ireplace(['and', 'or'], '', $statement['joiner'])) : $statement['joiner'];

            if ($joiner !== '') {
                $criteria[] = $joiner;
            }

            if (isset($statement['columns']) === true) {
                $criteria[] = sprintf('(%s)', $this->arrayStr((array) $statement['columns']));
                continue;
            }

            $key = $statement['key'];

            if ($key instanceof Raw === false) {
                $key = $this->wrapSanitizer($key);

                // Add alias non-existing
                if (is_string($key) && ! empty($this->aliasPrefix) && strpos($key, '.') === false) {
                    $key = $this->aliasPrefix . '.' . $key;
                }
            } else {
                $bindings[] = $key->getBindings();
            }

            $value = $statement['value'];

            if ($value instanceof Raw) {
                $bindings[] = $value->getBindings();
            }

            if ($value === null && $key instanceof Closure) {

                /**
                 * We have a closure, a nested criteria
                 *
                 * Build a new NestedCriteria class, keep it by reference so any changes
                 * made in the closure should reflect here
                 */

                $nestedCriteria = new NestedCriteria($this->connection);

                // Call the closure with our new nestedCriteria object
                $key($nestedCriteria);

                // Get the criteria only query from the nestedCriteria object
                $queryObject = $nestedCriteria->getQuery('criteriaOnly', true);

                // Merge the bindings we get from nestedCriteria object
                $bindings[] = $queryObject->getBindings();

                // Append the sql we get from the nestedCriteria object
                $criteria[] = sprintf('(%s)', $queryObject->getSql());

                continue;
            }

            if (is_array($value) === true) {
                // Where in or between like query
                $criteria[] = sprintf('%s %s', $key, $statement['operator']);

                if ($statement['operator'] === 'BETWEEN' || $statement['operator'] === 'NOT BETWEEN') {
                    $bindings[] = $statement['value'];
                    $criteria[] = '? AND ?';
                    continue;
                }

                $valuePlaceholder = '';

                foreach ((array) $statement['value'] as $subValue) {
                    $valuePlaceholder .= '?, ';
                    $bindings[]        = [$subValue];
                }

                $valuePlaceholder = trim($valuePlaceholder, ', ');
                $criteria[]       = sprintf('(%s)', $valuePlaceholder);

                continue;
            }

            if ($bindValues === false || $value instanceof Raw) {
                // Usual where like criteria specially for joins - we are not binding values, lets sanitize then
                $value      = $bindValues === false ? $this->wrapSanitizer($value) : $value;
                $criteria[] = sprintf('%s %s %s', $key, $statement['operator'], $value);

                if ($value instanceof Raw) {
                    $bindings[] = $value->getBindings();
                }

                continue;
            }

            if ($key instanceof Raw) {
                if ($statement['operator'] !== null) {
                    $criteria[] = sprintf('%s %s ?', $key, $statement['operator']);
                    $bindings[] = [$value];
                    continue;
                }

                $criteria[] = $key;
                continue;
            }

            // Check for objects that implement the __toString() magic method
            if (is_object($value) === true && method_exists($value, '__toString') === true) {
                $value = $value->__toString();
            }

            // WHERE
            $bindings[] = [$value];
            $criteria[] = sprintf('%s %s ?', $key, $statement['operator']);
        }

        // Clear all white spaces, and, or from beginning and white spaces from ending

        return [
            implode(' ', $criteria),
            array_merge(...$bindings),
        ];
    }

    /**
     * Build criteria string and binding with various types added, like WHERE and Having
     *
     * @param array<string, mixed> $statements
     *
     * @return array{string, array<string|int|float>}
     */
    protected function buildCriteriaWithType(
        array $statements,
        string $key,
        string $type,
        bool $bindValues = true
    ): array {
        $criteria = '';
        $bindings = [];

        if (isset($statements[$key]) === true) {
            // Get the generic/adapter agnostic criteria string from parent
            [$criteria, $bindings] = $this->buildCriteria($statements[$key], $bindValues);

            if ($criteria !== null) {
                $criteria = $type . ' ' . $criteria;
            }
        }

        return [$criteria, $bindings];
    }

    /**
     * Build join string
     *
     * @param array<string, mixed>   $statements
     * @param string[]|int[]|float[] $bindings
     */
    protected function buildJoin(array $statements, array &$bindings): string
    {
        $sql = '';

        $newBindings = [];

        if (isset($statements['joins']) === false) {
            return $sql;
        }

        foreach ((array) $statements['joins'] as $joinArr) {
            if (is_array($joinArr['table']) === true) {
                [$mainTable, $aliasTable] = $joinArr['table'];

                $table = $this->wrapSanitizer($mainTable) . ' AS ' . $this->wrapSanitizer($aliasTable);
            } else {
                $table = $joinArr['table'] instanceof Raw
                    ? (string) $joinArr['table']
                    : $this->wrapSanitizer($joinArr['table']);
            }

            $joinBuilder = $joinArr['joinBuilder'];

            $valueSql = '';

            if ($joinBuilder instanceof QueryBuilder) {
                $valueQuery    = $joinBuilder->getQuery('criteriaOnly', false);
                $valueSql      = $valueQuery->getSql();
                $newBindings[] = $valueQuery->getBindings();
            }

            $sqlArr = [
                $sql,
                strtoupper($joinArr['type']),
                'JOIN',
                $table,
                $valueSql,
            ];

            $sql = $this->concatenateQuery($sqlArr);
        }

        $bindings = array_merge($bindings, ...$newBindings);

        return $sql;
    }

    /**
     * Return table name with alias
     * e.g. foo as f
     *
     * @param array<string, mixed> $statements
     */
    protected function buildAliasedTableName(string $table, array $statements): string
    {
        $this->aliasPrefix = $statements['aliases'][$table] ?? null;
        if ($this->aliasPrefix !== null) {
            return sprintf(
                '%s AS %s',
                $this->wrapSanitizer($table),
                $this->wrapSanitizer(strtolower($this->aliasPrefix))
            );
        }

        return sprintf('%s', $this->wrapSanitizer($table));
    }

    /** @inheritDoc */
    public function criteriaOnly(array $statements, bool $bindValues = true): array
    {
        $sql = $bindings = [];
        if (isset($statements['criteria']) === false) {
            // phpcs:ignore Generic.PHP.ForbiddenFunctions.Found
            return compact('sql', 'bindings');
        }

        [$sql, $bindings] = $this->buildCriteria($statements['criteria'], $bindValues);

        // phpcs:ignore Generic.PHP.ForbiddenFunctions.Found
        return compact('sql', 'bindings');
    }

    /** @inheritDoc */
    public function delete(array $statements, ?array $columns = null): array
    {
        $table = end($statements['tables']);

        $columnsQuery = '';

        if ($columns !== null) {
            $columnsQuery = $this->arrayStr($columns);
        }

        // WHERE
        [$whereCriteria, $bindings] = $this->buildCriteriaWithType($statements, 'wheres', 'WHERE');

        $sql = $this->concatenateQuery([
            'DELETE ',
            $columnsQuery,
            ' FROM',
            $this->wrapSanitizer($table),
            $this->buildQueryPart(self::QUERY_PART_JOIN, $statements, $bindings),
            $whereCriteria,
            $this->buildQueryPart(self::QUERY_PART_GROUPBY, $statements, $bindings),
            $this->buildQueryPart(self::QUERY_PART_ORDERBY, $statements, $bindings),
            $this->buildQueryPart(self::QUERY_PART_LIMIT, $statements, $bindings),
            $this->buildQueryPart(self::QUERY_PART_OFFSET, $statements, $bindings),
        ]);

        // phpcs:ignore Generic.PHP.ForbiddenFunctions.Found
        return compact('sql', 'bindings');
    }

    /**
     * Build a generic insert/ignore/replace query
     *
     * @param array<string, mixed> $statements
     * @param array<string, mixed> $data
     *
     * @return array{sql: string, bindings: array<string|int|float>}
     */
    private function doInsert(array $statements, array $data, string $type): array
    {
        $table = end($statements['tables']);

        $bindings = $keys = $values = [];

        foreach ($data as $key => $value) {
            $keys[] = $key;
            if ($value instanceof Raw) {
                $values[]  = (string) $value;
                $bindings += $value->getBindings();
            } else {
                $values[]   = '?';
                $bindings[] = $value;
            }
        }

        $sqlArray = [
            $type . ' INTO',
            $this->wrapSanitizer($table),
            '(' . $this->arrayStr($keys) . ')',
            'VALUES',
            '(' . $this->arrayStr($values, ', ', false) . ')',
        ];

        if (isset($statements['onduplicate']) === true) {
            if (count($statements['onduplicate']) < 1) {
                throw new DatabaseException('No data given.', 4);
            }

            [$updateStatement, $updateBindings] = $this->getUpdateStatement($statements['onduplicate']);
            $sqlArray[]                         = 'ON DUPLICATE KEY UPDATE ' . $updateStatement;
            $bindings                           = array_merge($bindings, $updateBindings);
        }

        $sql = $this->concatenateQuery($sqlArray);

        // phpcs:ignore Generic.PHP.ForbiddenFunctions.Found
        return compact('sql', 'bindings');
    }

    /**
     * Build fields assignment part of SET ... or ON DUPLICATE KEY UPDATE ... statements
     *
     * @param array<string, mixed> $data
     *
     * @return array{string, array<string|int|float>}
     */
    private function getUpdateStatement(array $data): array
    {
        $bindings   = [];
        $statements = [];

        foreach ($data as $key => $value) {
            $statement = $this->wrapSanitizer($key) . ' = ';

            if ($value instanceof Raw) {
                $statements[] = $statement . $value;
                $bindings    += $value->getBindings();
            } else {
                $statements[] = $statement . '?';
                $bindings[]   = $value;
            }
        }

        $statement = trim($this->arrayStr($statements, ', ', false));

        return [$statement, $bindings];
    }

    /** @inheritDoc*/
    public function insert(array $statements, array $data): array
    {
        return $this->doInsert($statements, $data, 'INSERT');
    }

    /** @inheritDoc*/
    public function insertIgnore(array $statements, array $data): array
    {
        return $this->doInsert($statements, $data, 'INSERT IGNORE');
    }

    /** @inheritDoc */
    public function replace(array $statements, array $data): array
    {
        return $this->doInsert($statements, $data, 'REPLACE');
    }

    /**
     * Sets select statements and returns status of distinct tables.
     *
     * @param array<string, mixed>    $statements
     * @param array<string|int|float> $bindings
     *
     * @return bool Returns true if distinct tables are found.
     */
    protected function setSelectStatement(array &$statements, array &$bindings): bool
    {
        $hasDistincts = false;

        if (isset($statements['distincts']) === true && count($statements['distincts']) > 0) {
            $hasDistincts = true;

            if (isset($statements['selects']) === true && count($statements['selects']) > 0) {
                $statements['selects'] = array_merge($statements['distincts'], $statements['selects']);
            } else {
                $statements['selects'] = $statements['distincts'];
            }
        } else {
            if (isset($statements['selects']) === false) {
                $statements['selects'] = ['*'];
            }
        }

        foreach ((array) $statements['selects'] as $select) {
            if (! ($select instanceof Raw)) {
                continue;
            }

            $bindings += $select->getBindings();
        }

        return $hasDistincts;
    }

    /** @inheritDoc*/
    public function select(array $statements): array
    {
        $bindings = [];

        $hasDistincts = $this->setSelectStatement($statements, $bindings);

        // From
        $fromEnabled = false;
        $tables      = '';

        if (isset($statements['tables']) === true) {
            $tablesFound = [];
            foreach ((array) $statements['tables'] as $table) {
                if ($table instanceof Raw) {
                    $t = $table;
                } else {
                    $t = $this->buildAliasedTableName($table, $statements);
                }

                $tablesFound[] = $t;
            }

            $tables      = implode(',', $tablesFound);
            $fromEnabled = true;
        }

        // WHERE
        [$whereCriteria, $whereBindings] = $this->buildCriteriaWithType($statements, 'wheres', 'WHERE');

        // HAVING
        [$havingCriteria, $havingBindings] = $this->buildCriteriaWithType($statements, 'havings', 'HAVING');

        $sql = $this->concatenateQuery([
            'SELECT' . ($hasDistincts === true ? ' DISTINCT' : ''),
            $this->arrayStr($statements['selects']),
            $fromEnabled ? 'FROM' : '',
            $tables,
            $this->buildQueryPart(self::QUERY_PART_JOIN, $statements, $bindings),
            $whereCriteria,
            $this->buildQueryPart(self::QUERY_PART_GROUPBY, $statements, $bindings),
            $havingCriteria,
            $this->buildQueryPart(self::QUERY_PART_ORDERBY, $statements, $bindings),
            $this->buildQueryPart(self::QUERY_PART_LIMIT, $statements, $bindings),
            $this->buildQueryPart(self::QUERY_PART_OFFSET, $statements, $bindings),
            $this->buildQueryPart(self::QUERY_PART_FOR, $statements, $bindings),
        ]);

        $sql = $this->buildUnion($statements, $sql);

        $bindings = array_merge(
            $bindings,
            $whereBindings,
            $havingBindings
        );

        // phpcs:ignore Generic.PHP.ForbiddenFunctions.Found
        return compact('sql', 'bindings');
    }

    /**
     * Returns specific part of a query like JOIN, LIMIT, OFFSET etc.
     *
     * @param array<string, mixed>    $statements
     * @param array<string|int|float> $bindings
     */
    protected function buildQueryPart(string $section, array $statements, array &$bindings): string
    {
        switch ($section) {
            case self::QUERY_PART_JOIN:
                return $this->buildJoin($statements, $bindings);

            case self::QUERY_PART_TOP:
                return isset($statements['limit']) ? 'TOP ' . $statements['limit'] : '';

            case self::QUERY_PART_LIMIT:
                return isset($statements['limit']) ? 'LIMIT ' . $statements['limit'] : '';

            case self::QUERY_PART_OFFSET:
                return isset($statements['offset']) ? 'OFFSET ' . $statements['offset'] : '';

            case self::QUERY_PART_ORDERBY:
                $orderBys = '';
                if (isset($statements['orderBys']) === true && is_array($statements['orderBys']) === true) {
                    foreach ($statements['orderBys'] as $orderBy) {
                        $orderBys .= $this->wrapSanitizer($orderBy['field']) . ' ' . $orderBy['type'] . ', ';
                    }

                    $orderBys = trim($orderBys, ', ');
                    if ($orderBys !== '') {
                        $orderBys = 'ORDER BY ' . $orderBys;
                    }
                }

                return $orderBys;

            case self::QUERY_PART_GROUPBY:
                $groupBys = $this->arrayStr($statements['groupBys']);
                if ($groupBys !== '' && isset($statements['groupBys']) === true) {
                    $groupBys = 'GROUP BY ' . $groupBys;
                }

                return $groupBys;

            case self::QUERY_PART_FOR:
                return isset($statements['for']) ? ' FOR ' . $statements['for'][0] : '';
        }

        return '';
    }

    /**
     * Adds union query to sql statement
     *
     * @param array<string, mixed> $statements
     */
    protected function buildUnion(array $statements, string $sql): string
    {
        if (isset($statements['unions']) === false || count($statements['unions']) === 0) {
            return $sql;
        }

        foreach ((array) $statements['unions'] as $i => $union) {
            $queryBuilder = $union['query'];
            assert($queryBuilder instanceof QueryBuilder);

            if ($i === 0) {
                $sql .= ')';
            }

            $type = $union['type'] !== QueryBuilder::UNION_TYPE_NONE ? $union['type'] . ' ' : '';
            $sql .= sprintf(' UNION %s(%s)', $type, $queryBuilder->getQuery('select')->getRawSql());
        }

        return sprintf('(%s', $sql);
    }

    /** @inheritDoc*/
    public function update(array $statements, array $data): array
    {
        if (count($data) === 0) {
            throw new DatabaseException('No data given.', 4);
        }

        $table = end($statements['tables']);

        // UPDATE
        [$updateStatement, $bindings] = $this->getUpdateStatement($data);

        // WHERE
        [$whereCriteria, $whereBindings] = $this->buildCriteriaWithType($statements, 'wheres', 'WHERE');

        $sqlArray = [
            'UPDATE',
            $this->buildAliasedTableName($table, $statements),
            $this->buildQueryPart(self::QUERY_PART_JOIN, $statements, $bindings),
            'SET ' . $updateStatement,
            $whereCriteria,
            $this->buildQueryPart(self::QUERY_PART_GROUPBY, $statements, $bindings),
            $this->buildQueryPart(self::QUERY_PART_ORDERBY, $statements, $bindings),
            $this->buildQueryPart(self::QUERY_PART_LIMIT, $statements, $bindings),
            $this->buildQueryPart(self::QUERY_PART_OFFSET, $statements, $bindings),
        ];

        $sql = $this->concatenateQuery($sqlArray);

        $bindings = array_merge($bindings, $whereBindings);

        return compact('sql', 'bindings'); // phpcs:ignore
    }

    /** @inheritDoc */
    public function wrapSanitizer($value)
    {
        // It's a raw query, just cast as string, object has __toString()
        if ($value instanceof Raw) {
            return (string) $value;
        }

        if ($value instanceof Closure) {
            return $value;
        }

        // Separate our table and fields which are joined with a ".", like my_table.id
        $valueArr = explode('.', $value, 2);

        foreach ($valueArr as $key => $subValue) {
            if (strpos($subValue, ' as ') !== false) {
                [$column, $as]  = $this->splitAliasStatement($subValue);
                $valueArr[$key] = sprintf('%s AS %s', $this->wrapSanitizer($column), $this->wrapSanitizer($as));
            } else {
                // Don't wrap if we have *, which is not a usual field
                $valueArr[$key] = trim($subValue) === '*' ? $subValue : self::SANITIZER . $subValue . self::SANITIZER;
            }
        }

        // Join these back with "." and return
        return implode('.', $valueArr);
    }

    /** @return string[] */
    protected function splitAliasStatement(string $value): array
    {
        return [...explode(' as ', $value)];
    }
}
