<?php

declare(strict_types=1);

namespace Devly\WPDB\Query;

use Closure;
use Devly\Exceptions\DatabaseException;
use Devly\WPDB\Connection;
use Devly\WPDB\Contracts\IQueryAdapter;
use Devly\WPDB\DatabaseConnection;
use Devly\WPDB\Event;
use Devly\WPDB\Exceptions\ColumnNotFound;
use Devly\WPDB\Exceptions\QueryBuilderException;
use Illuminate\Support\Collection;
use InvalidArgumentException;
use stdClass;
use Throwable;

use function apply_filters;
use function array_key_exists;
use function array_merge;
use function array_shift;
use function array_values;
use function call_user_func;
use function collect;
use function compact;
use function count;
use function current;
use function do_action;
use function end;
use function explode;
use function func_get_args;
use function func_num_args;
use function gettype;
use function in_array;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;
use function microtime;
use function sprintf;
use function strpos;
use function strtolower;
use function strtoupper;
use function substr_replace;

class QueryBuilder
{
    public const FILTER_TABLE_PREFIX         = 'devly/wpdb/db/add_table_prefix';
    public const ACTION_BEFORE_SELECT        = 'devly/wpdb/db/before_select';
    public const ACTION_AFTER_SELECT         = 'devly/wpdb/db/after_select';
    public const ACTION_BEFORE_SELECT_ROW    = 'devly/wpdb/db/before_select_row';
    public const ACTION_AFTER_SELECT_ROW     = 'devly/wpdb/db/after_select_row';
    public const ACTION_BEFORE_DELETE        = 'devly/wpdb/db/before_delete';
    public const ACTION_AFTER_DELETE         = 'devly/wpdb/db/after_delete';
    public const ACTION_BEFORE_INSERT        = 'devly/wpdb/db/before_insert';
    public const ACTION_AFTER_INSERT         = 'devly/wpdb/db/after_insert';
    public const ACTION_BEFORE_QUERY         = 'devly/wpdb/db/before_query';
    public const ACTION_AFTER_QUERY          = 'devly/wpdb/db/after_query';
    public const ACTION_BEFORE_UPDATE        = 'devly/wpdb/db/before_update';
    public const ACTION_AFTER_UPDATE         = 'devly/wpdb/db/after_update';
    public const ACTION_BEFORE_SELECT_COLUMN = 'devly/wpdb/db/before_select_column';
    public const ACTION_AFTER_SELECT_COLUMN  = 'devly/wpdb/db/after_select_column';
    public const ACTION_BEFORE_SELECT_VALUE  = 'devly/wpdb/db/before_select_value';
    public const ACTION_AFTER_SELECT_VALUE   = 'devly/wpdb/db/after_select_value';

    /**
     * Default union type
     */
    public const UNION_TYPE_NONE = '';

    /**
     * Union type distinct
     */
    public const UNION_TYPE_DISTINCT = 'DISTINCT';

    /**
     * Union type all
     */
    public const UNION_TYPE_ALL = 'ALL';

    protected ?Connection $connection;

    /** @var array{groupBys: string[], unions: array<array{query: string, type: string}>, selects?: string[]|Raw[], tables?: string|string[]|null, aliases?: string[]} */
    protected array $statements = [
        'groupBys' => [],
        'unions'   => [],
    ];

    protected ?string $tablePrefix;

    protected IQueryAdapter $adapter;

    /**
     * If true calling from, select etc. will overwrite any existing
     * values from previous calls in query.
     */
    protected bool $overwriteEnabled = false;

    /** @throws DatabaseException If database connection missing. */
    final public function __construct(?Connection $connection = null)
    {
        $this->connection = $connection ?? Connection::getStoredConnection();

        if ($this->connection === null) {
            throw new DatabaseException('No database connection found.');
        }

        $tablePrefix = $this->connection->getTablePrefix();
        if (! empty($tablePrefix)) {
            $this->tablePrefix = $tablePrefix;
        }

        // Query builder adapter instance
        $this->adapter = $this->connection->getQueryAdapter();
    }

    /**
     * Get count of all the rows for the current query
     *
     * @throws ColumnNotFound
     * @throws DatabaseException
     */
    public function count(string $field = '*'): int
    {
        return (int) $this->aggregate('count', $field);
    }

    /**
     * Performs special queries like COUNT, SUM etc. based on the current query.
     *
     * @throws ColumnNotFound
     * @throws DatabaseException If no table selected.
     */
    protected function aggregate(string $type, string $field = '*'): float
    {
        // Verify that field exists
        if (
            $field !== '*'
            && isset($this->statements['selects']) === true
            && in_array($field, $this->statements['selects'], true) === false
        ) {
            throw new ColumnNotFound(sprintf(
                'Failed to count query - the column %s hasn\'t been selected in the query.',
                $field
            ));
        }

        if (isset($this->statements['tables']) === false) {
            throw new DatabaseException('No table selected');
        }

        $query = $this
            ->table($this->subQuery($this, 'count'))
            ->select([
                $this->raw(sprintf(
                    '%s(%s) AS ' . $this->getQueryAdapter()->wrapSanitizer('field'),
                    strtoupper($type),
                    $field
                )),
            ]);

        $queryObject = $query->getQuery();
        $this->connection->setLastQuery($queryObject);

        return (float) $this->db()->getVar($queryObject->getRawSql());
    }

    /**
     * Adds fields to select on the current query (defaults is all).
     * You can use key/value array to create alias.
     * Sub-queries and raw-objects are also supported.
     *
     * Example: ['field' => 'alias'] will become `field` AS `alias`
     *
     * @param string|array<int|string, string|Raw>|Raw $fields,...
     *
     * @return static
     */
    public function select($fields): self
    {
        $fields = is_array($fields) ? $fields : func_get_args();

        $fields = $this->addTablePrefix($fields);

        if ($this->overwriteEnabled === true) {
            $this->statements['selects'] = $fields;
        } else {
            $this->addStatement('selects', $fields);
        }

        return $this;
    }

    /**
     * Adds raw fields to select on the current query.
     *
     * @param array<string|int|float> $bindings
     *
     * @return static
     */
    public function selectRaw(string $sql, array $bindings = []): self
    {
        return $this->select($this->raw($sql, $bindings));
    }

    /**
     * Add table prefix (if given) on given string.
     *
     * @param string|array<int|string, mixed>|Raw|Closure $values
     * @param bool                                        $tableFieldMix If we have mixes of field and table names
     *                                                                   with a "."
     *
     * @return string[]|string
     */
    public function addTablePrefix($values, bool $tableFieldMix = true)
    {
        $tablePrefix = apply_filters(self::FILTER_TABLE_PREFIX, $this->tablePrefix ?? false);

        if ($tablePrefix === null || $tablePrefix === false) {
            return $values;
        }

        // $value will be an array, and we will add prefix to all table names
        // If supplied value is not an array then make it one

        $single = false;
        if (is_array($values) === false) {
            $values = [$values];

            // We had single value, so should return a single value
            $single = true;
        }

        $return = [];

        foreach ($values as $key => $value) {
            // It's a raw query, just add it to our return array and continue next
            if ($value instanceof Raw || $value instanceof Closure) {
                $return[$key] = $value;
                continue;
            }

            // If key is not integer, it is likely an alias mapping, so we need to change prefix target
            $target = &$value;

            if (is_int($key) === false) {
                $target = &$key;
            }

            if (($tableFieldMix === false) || (strpos($target, '.') !== false)) {
                $target = $tablePrefix . $target;
            }

            $return[$key] = $value;
        }

        // If we had single value then we should return a single value (end value of the array)
        return $single ? end($return) : $return;
    }

    /**
     * Add new statement to statement-list
     *
     * @param string|array<string, mixed> $value
     */
    protected function addStatement(string $key, $value): void
    {
        if (array_key_exists($key, $this->statements) === false) {
            $this->statements[$key] = (array) $value;
        } else {
            $this->statements[$key] = array_merge($this->statements[$key], (array) $value);
        }
    }

    /**
     * Sets the table that the query is using
     * Note: to remove a table set the $tables argument to null.
     *
     * @param string|list<string>|Raw|null $tables Single table or multiple tables as an array or as multiple parameters
     *
     * @return static
     *
     * ```
     * Examples:
     *  - basic usage
     * ->table('table_one')
     * ->table(['table_one'])
     *
     *  - with aliasing
     * ->table(['table_one' => 'one'])
     * ->table($qb->raw('table_one as one'))
     * ```
     */
    public function table($tables = null): self
    {
        if ($tables === null) {
            return $this->from($tables);
        }

        // phpcs:ignore PHPCompatibility.FunctionUse.ArgumentFunctionsReportCurrentValue.NeedsInspection
        $tables = is_array($tables) || func_num_args() === 1 ? $tables : func_get_args();

        return $this->newQuery()->from($tables);
    }

    /**
     * Adds FROM statement to the current query.
     *
     * @param string|array<string|int, string>|null $tables Single table or multiple tables as an array or as
     *                                                      multiple parameters
     *
     * @return static
     */
    public function from($tables = null): self
    {
        if (empty($tables)) {
            $this->statements['tables'] = null;

            return $this;
        }

        // phpcs:ignore PHPCompatibility.FunctionUse.ArgumentFunctionsReportCurrentValue.NeedsInspection
        $tables = is_array($tables) ? $tables : func_get_args();

        $tTables = [];

        foreach ($tables as $key => $value) {
            if (is_string($key) === true) {
                $this->alias($value, $key);
                $tTables[] = $key;
                continue;
            }

            $tTables[] = $value;
        }

        $tTables                    = $this->addTablePrefix($tTables, false);
        $this->statements['tables'] = $tTables;

        return $this;
    }

    /**
     * Add or change table alias
     * Example: table AS alias
     *
     * @return static
     */
    public function alias(string $alias, ?string $table = null): self
    {
        if ($table === null && isset($this->statements['tables'][0]) === true) {
            $table = $this->statements['tables'][0];
        } else {
            $table = $this->tablePrefix . $table;
        }

        $this->statements['aliases'][$table] = strtolower($alias);

        return $this;
    }

    /**
     * Creates and returns new query.
     *
     * @return static
     */
    public function newQuery(): self
    {
        return new static($this->connection);
    }

    /**
     * Performs new sub-query.
     * Call this method when you want to add a new sub-query in your where etc.
     */
    public function subQuery(QueryBuilder $queryBuilder, ?string $alias = null): Raw
    {
        $sql = '(' . $queryBuilder->getQuery()->getRawSql() . ')';
        if ($alias !== null) {
            $sql .= ' AS ' . $this->adapter->wrapSanitizer($alias);
        }

        return $queryBuilder->raw($sql);
    }

    /**
     * Returns Query-object.
     *
     * @param mixed $arguments
     */
    public function getQuery(string $type = 'select', ...$arguments): QueryObject
    {
        $allowedTypes = [
            'select',
            'insert',
            'insertignore',
            'replace',
            'delete',
            'update',
            'criteriaonly',
        ];

        if (in_array(strtolower($type), $allowedTypes, true) === false) {
            throw new DatabaseException($type . ' is not a known type.', 1);
        }

        $queryArr = $this->adapter->$type($this->statements, ...$arguments);

        return new QueryObject($queryArr['sql'], $queryArr['bindings'], $this->getConnection());
    }

    /**
     * Get connection object
     */
    public function getConnection(): Connection
    {
        return $this->connection;
    }

    /**
     * Set connection object
     */
    public function setConnection(Connection $connection): self
    {
        $this->connection = $connection;

        return $this;
    }

    /**
     * Adds a raw string to the current query.
     *
     * This query will be ignored from any parsing or formatting by the Query builder
     * and should be used in conjunction with other statements in the query.
     *
     * For example: $qb->where('result', '>', $qb->raw('COUNT(`score`)));
     *
     * @param array<string|int|float>|mixed|null $bindings
     */
    public function raw(string $value, $bindings = null): Raw
    {
        if (is_array($bindings) === false) {
            // phpcs:ignore PHPCompatibility.FunctionUse.ArgumentFunctionsReportCurrentValue.NeedsInspection
            $bindings = func_get_args();
            array_shift($bindings);
        }

        return new Raw($value, $bindings);
    }

    public function db(): DatabaseConnection
    {
        return $this->getConnection()->getDatabaseConnection();
    }

    /**
     * Get the alias for the current query
     */
    public function getAlias(): ?string
    {
        return isset($this->statements['aliases']) === true ? array_values($this->statements['aliases'])[0] : null;
    }

    /**
     * Get the table-name for the current query
     */
    public function getTable(): ?string
    {
        if (isset($this->statements['tables']) === true) {
            $table = array_values($this->statements['tables'])[0];
            if ($table instanceof Raw === false) {
                return $table;
            }
        }

        return null;
    }

    /**
     * Returns the first row
     *
     * @return stdClass|array<string, mixed>|null
     */
    public function first(string $output = OBJECT)
    {
        return $this->row($output);
    }

    /**
     * Get all rows
     *
     * @return array<string, mixed>|object|null
     */
    public function row(string $output = OBJECT)
    {
        $queryObject = $this->getQuery();
        $this->connection->setLastQuery($queryObject);

        $this->fireEvents(self::ACTION_BEFORE_SELECT_ROW, $queryObject);

        $executionTime = 0.0;
        $startTime     = microtime(true);

        $statement = $this->statement(
            $queryObject->getSql(),
            $queryObject->getBindings()
        );

        $result = $this->db()->getRow($statement, $output);

        $executionTime += microtime(true) - $startTime;

        $this->fireEvents(self::ACTION_AFTER_SELECT_ROW, $queryObject, ['execution_time' => $executionTime]);

        return $result;
    }

    /**
     * Fires event by given event name
     *
     * @param mixed[] $eventArguments
     */
    public function fireEvents(string $name, QueryObject $queryObject, array $eventArguments = []): void
    {
        do_action($name, new Event($name, $queryObject, $this, $eventArguments));
    }

    /**
     * Execute statement
     *
     * @param array<string|int|float> $bindings
     */
    public function statement(string $sql, array $bindings = []): string
    {
        if (strpos($sql, '?') === false) {
            return $sql;
        }

        foreach ($bindings as $binding) {
            $pos = strpos($sql, '?');
            if ($pos === false) {
                continue;
            }

            if (is_string($binding)) {
                $replacement = '%s';
            } elseif (is_float($binding)) {
                $replacement = '%f';
            } elseif (is_int($binding)) {
                $replacement = '%d';
            } else {
                throw new InvalidArgumentException(sprintf('Value type "%s" not allowed', gettype($binding)));
            }

            $sql = substr_replace($sql, $replacement, $pos, 1);
        }

        return $this->db()->prepare($sql, ...$bindings);
    }

    /**
     * Returns the last row
     *
     * @return stdClass|array<string, mixed>|null
     */
    public function last(string $output = OBJECT)
    {
        return $this->orderByDesc('ID')->row($output);
    }

    /**
     * Adds ORDER BY DESC statement to the current query.
     *
     * @param string|string[]|Raw|Closure $fields
     */
    public function orderByDesc($fields): self
    {
        return $this->orderBy($fields, 'DESC');
    }

    /**
     * Adds ORDER BY statement to the current query.
     *
     * @param string|Raw|Closure|string[] $fields
     */
    public function orderBy($fields, string $direction = 'ASC'): self
    {
        if (is_array($fields) === false) {
            $fields = [$fields];
        }

        foreach ($fields as $key => $value) {
            $field = $key;
            $type  = $value;

            if (is_int($key) === true) {
                $field = $value;
                $type  = $direction;
            }

            if (($field instanceof Raw) === false) {
                $field = $this->addTablePrefix($field);
            }

            $this->removeExistingStatement('orderBys', 'field', $field);

            // phpcs:ignore Generic.PHP.ForbiddenFunctions.Found
            $this->statements['orderBys'][] = compact('field', 'type');
        }

        return $this;
    }

    /**
     * Removes existing statement if overwrite is set to enable.
     *
     * @param string $type  Statement type
     * @param string $key   Key to search for
     * @param mixed  $value Value to match
     */
    protected function removeExistingStatement(string $type, string $key, $value): void
    {
        if ($this->overwriteEnabled === false || isset($this->statements[$type]) === false) {
            return;
        }

        foreach ($this->statements[$type] as $index => $statement) {
            if ($statement[$key] instanceof Closure) {
                $nestedCriteria = $this->getConnection()->createQueryBuilder();

                $statement[$key]($nestedCriteria);

                if (isset($nestedCriteria->getStatements()[$type])) {
                    foreach ($nestedCriteria->getStatements()[$type] as $subStatement) {
                        if ($subStatement[$key] === $value) {
                            unset($this->statements[$type][$index]);

                            return;
                        }
                    }
                }
            }

            if ($statement[$key] === $value) {
                unset($this->statements[$type][$index]);

                return;
            }
        }
    }

    /**
     * Returns statements
     *
     * @return array{groupBys: string[], unions: array<array{query: string, type: string}>, selects?: string[]|Raw[], tables?: string|string[]|null, aliases?: string[]}
     */
    public function getStatements(): array
    {
        return $this->statements;
    }

    /** @param array{groupBys: string[], unions: array<array{query: string, type: string}>, selects?: string[]|Raw[], tables?: string|string[]|null, aliases?: string[]} $statements */
    public function setStatements(array $statements): self
    {
        $this->statements = $statements;

        return $this;
    }

    public function value(string $column): ?string
    {
        $this->select($column);
        $queryObject = $this->getQuery();
        $this->connection->setLastQuery($queryObject);

        $this->fireEvents(self::ACTION_BEFORE_SELECT_VALUE, $queryObject);

        $executionTime = 0.0;
        $startTime     = microtime(true);

        $statement = $this->statement(
            $queryObject->getSql(),
            $queryObject->getBindings()
        );

        $result = $this->db()->getVar($statement);

        $executionTime += microtime(true) - $startTime;

        $this->fireEvents(self::ACTION_AFTER_SELECT_VALUE, $queryObject, ['execution_time' => $executionTime]);

        return $result;
    }

    /** @return array<array-key, mixed> */
    public function pluck(string $column): array
    {
        $this->select($column);
        $queryObject = $this->getQuery();
        $this->connection->setLastQuery($queryObject);

        $this->fireEvents(self::ACTION_BEFORE_SELECT_COLUMN, $queryObject);

        $executionTime = 0.0;
        $startTime     = microtime(true);

        $statement = $this->statement(
            $queryObject->getSql(),
            $queryObject->getBindings()
        );

        $result = $this->db()->getCol($statement);

        $executionTime += microtime(true) - $startTime;

        $this->fireEvents(self::ACTION_AFTER_SELECT_COLUMN, $queryObject, ['execution_time' => $executionTime]);

        return $result;
    }

    public function exists(): bool
    {
        return $this->row() !== null;
    }

    public function doesntExist(): bool
    {
        return $this->row() === null;
    }

    /**
     * Get query-object from last executed query.
     */
    public function getLastQuery(): ?QueryObject
    {
        return $this->connection->getLastQuery();
    }

    public function take(int $limit): self
    {
        return $this->limit($limit);
    }

    /**
     * Adds LIMIT statement to the current query.
     *
     * @return static
     */
    public function limit(int $limit): self
    {
        $this->statements['limit'] = $limit;

        return $this;
    }

    /**
     * Adds FETCH NEXT statement to the current query.
     *
     * @return static $this
     */
    public function fetchNext(int $fetchNext): self
    {
        $this->statements['fetch_next'] = $fetchNext;

        return $this;
    }

    /**
     * Get the sum for a field in the current query
     */
    public function sum(string $field): float
    {
        return $this->aggregate('sum', $field);
    }

    /**
     * Alias to avg() method
     */
    public function average(string $field): float
    {
        return $this->avg($field);
    }

    /**
     * Get the average for a field in the current query
     */
    public function avg(string $field): float
    {
        return $this->aggregate('avg', $field);
    }

    /**
     * Get the minimum for a field in the current query
     */
    public function min(string $field): float
    {
        return $this->aggregate('min', $field);
    }

    /**
     * Get the maximum for a field in the current query
     */
    public function max(string $field): float
    {
        return $this->aggregate('max', $field);
    }

    /**
     * Forms delete on the current query.
     *
     * @param string[]|null $columns
     */
    public function delete(?array $columns = null): int
    {
        $queryObject = $this->getQuery('delete', $columns);

        $this->getConnection()->setLastQuery($queryObject);

        $this->fireEvents(self::ACTION_BEFORE_DELETE, $queryObject);

        $executionTime = 0.0;
        $startTime     = microtime(true);

        $response = $this->db()->query($this->statement($queryObject->getSql(), $queryObject->getBindings()));

        $executionTime += microtime(true) - $startTime;

        $this->fireEvents(self::ACTION_AFTER_DELETE, $queryObject, ['execution_time' => $executionTime]);

        return $response;
    }

    /**
     * Find by value and field name.
     *
     * @param string|int|float $value
     */
    public function find($value, string $fieldName = 'id'): ?stdClass
    {
        return $this->where($fieldName, '=', $value)->row();
    }

    /**
     * Adds WHERE statement to the current query.
     *
     * @param string|Raw|Closure            $key
     * @param string|mixed|Raw|Closure|null $operator
     * @param mixed|Raw|Closure|null        $value
     *
     * @return static
     */
    public function where($key, $operator = null, $value = null): self
    {
        // If two params are given then assume operator is =
        if (func_num_args() === 2) {
            $value    = $operator;
            $operator = '=';
        }

        if (is_bool($value) === true) {
            $value = (int) $value;
        }

        return $this->whereHandler($key, $operator, $value);
    }

    /**
     * @param array<string|int|float> $bindings
     *
     * @return static
     */
    public function whereRaw(string $sql, array $bindings = []): self
    {
        return $this->whereHandler($this->raw($sql, $bindings));
    }

    /**
     * Handles where statements
     *
     * @param string|Raw|Closure            $key
     * @param string|array|Raw|Closure|null $value
     *
     * @return static
     */
    protected function whereHandler($key, ?string $operator = null, $value = null, string $joiner = 'AND'): self
    {
        $key = $this->addTablePrefix($key);
        $this->removeExistingStatement('wheres', 'key', $key);
        // phpcs:ignore Generic.PHP.ForbiddenFunctions.Found
        $this->statements['wheres'][] = compact('key', 'operator', 'value', 'joiner');

        return $this;
    }

    /**
     * Adds AND WHERE statement to the current query.
     *
     * @param string|Raw|Closure            $key
     * @param string|mixed|Raw|Closure|null $operator
     * @param mixed|Raw|Closure|null        $value
     *
     * @return static
     */
    public function andWhere($key, $operator = null, $value = null): self
    {
        // If two params are given then assume operator is =
        if (func_num_args() === 2) {
            $value    = $operator;
            $operator = '=';
        }

        if (is_bool($value) === true) {
            $value = (int) $value;
        }

        return $this->whereHandler($key, $operator, $value);
    }

    /**
     * Find all by field name and value
     *
     * @param string|int|float $value
     */
    public function findAll(string $fieldName, $value): Collection
    {
        return $this->where($fieldName, '=', $value)->get();
    }

    /**
     * Get all rows
     */
    public function get(string $output = OBJECT): Collection
    {
        $queryObject = $this->getQuery();
        $this->connection->setLastQuery($queryObject);

        $this->fireEvents(self::ACTION_BEFORE_SELECT, $queryObject);

        $executionTime = 0.0;
        $startTime     = microtime(true);

        $statement = $this->statement($queryObject->getSql(), $queryObject->getBindings());

        $result = $this->db()->getResults($statement, $output);

        $executionTime += microtime(true) - $startTime;

        $this->fireEvents(self::ACTION_AFTER_SELECT, $queryObject, ['execution_time' => $executionTime]);

        return collect($result);
    }

    /**
     * Adds GROUP BY to the current query.
     *
     * @param string|Raw|Closure|array<int|string, mixed> $field
     *
     * @return static
     */
    public function groupBy($field): self
    {
        if (($field instanceof Raw) === false) {
            $field = $this->addTablePrefix($field);
        }

        if ($this->overwriteEnabled === true) {
            $this->statements['groupBys'] = [];
        }

        if (is_array($field) === true) {
            $this->statements['groupBys'] = array_merge($this->statements['groupBys'], $field);
        } else {
            $this->statements['groupBys'][] = $field;
        }

        return $this;
    }

    /**
     * Adds new INNER JOIN statement to the current query.
     *
     * @param string|Raw|Closure             $table
     * @param string|JoinBuilder|Raw|Closure $key
     * @param string|mixed|null              $operator
     * @param string|Raw|Closure|null        $value
     *
     * @return static
     */
    public function innerJoin($table, $key, $operator = null, $value = null): self
    {
        return $this->join($table, $key, $operator, $value, 'inner');
    }

    /**
     * Adds new JOIN statement to the current query.
     *
     * @param string|Raw|Closure|string[]         $table
     * @param string|JoinBuilder|Raw|Closure|null $key
     * @param string|Raw|Closure                  $value
     *
     * @return static
     *
     * ```
     * Examples:
     * - basic usage
     * ->join('table2', 'table2.person_id', '=', 'table1.id');
     *
     * - as alias 'bar'
     * ->join(['table2','bar'], 'bar.person_id', '=', 'table1.id');
     *
     * - complex usage
     * ->join('another_table', function($table)
     * {
     *  $table->on('another_table.person_id', '=', 'my_table.id');
     *  $table->on('another_table.person_id2', '=', 'my_table.id2');
     *  $table->orOn('another_table.age', '>', $queryBuilder->raw(1));
     * })
     * ```
     */
    public function join($table, $key = null, ?string $operator = null, $value = null, string $type = ''): self
    {
        $joinBuilder = null;

        if ($key !== null) {
            $joinBuilder = new JoinBuilder($this->connection);

            /**
             * Build a new JoinBuilder class, keep it by reference so any changes made
             * in the closure should reflect here
             */
            if ($key instanceof Closure === false) {
                $key = static function (JoinBuilder $joinBuilder) use ($key, $operator, $value): void {
                    $joinBuilder->on($key, $operator, $value);
                };
            }

            // Call the closure with our new joinBuilder object
            $key($joinBuilder);
        }

        $table = $this->addTablePrefix($table, false);

        $this->removeExistingStatement('joins', 'table', $table);

        // Get the criteria only query from the joinBuilder object
        $this->statements['joins'][] = [
            'type'        => $type,
            'table'       => $table,
            'joinBuilder' => $joinBuilder,
        ];

        return $this;
    }

    /**
     * Insert with ignore key/value array
     *
     * @param array<string|int, mixed> $data
     *
     * @return array<int|string, mixed>|int|null
     */
    public function insertIgnore(array $data)
    {
        return $this->doInsert($data, 'insertignore');
    }

    /**
     * Performs insert
     *
     * @param array<string|int, mixed> $data
     *
     * @return array|int|null
     */
    private function doInsert(array $data, string $type)
    {
        // Insert single item

        if (is_array(current($data)) === false) {
            $queryObject = $this->getQuery($type, $data);

            $this->connection->setLastQuery($queryObject);

            $this->fireEvents(self::ACTION_BEFORE_INSERT, $queryObject);

            $executionTime = 0.0;
            $startTime     = microtime(true);

            $sql   = $this->statement($queryObject->getSql(), $queryObject->getBindings());
            $count = $this->db()->query($sql);

            $insertId = $count === 1 ? $this->db()->lastInsertId() : null;

            $executionTime += microtime(true) - $startTime;

            $this->fireEvents(self::ACTION_AFTER_INSERT, $queryObject, [
                'insert_id'      => $insertId,
                'execution_time' => $executionTime,
            ]);

            return $insertId;
        }

        $insertIds = [];

        // If the current batch insert is not in a transaction, we create one...

        if ($this->db()->inTransaction() === false) {
            $this->transaction(static function (Transaction $transaction) use (&$insertIds, $data, $type): void {
                foreach ($data as $subData) {
                    $insertIds[] = $transaction->doInsert($subData, $type);
                }
            });

            return $insertIds;
        }

        // Otherwise, insert one by one...
        foreach ($data as $subData) {
            $insertIds[] = $this->doInsert($subData, $type);
        }

        return $insertIds;
    }

    /**
     * Performs the transaction
     */
    public function transaction(Closure $callback): Transaction
    {
        $queryTransaction             = new Transaction($this->connection);
        $queryTransaction->statements = $this->statements;

        try {
            // Begin the PDO transaction
            if ($this->db()->inTransaction() === false) {
                $this->db()->beginTransaction();
            }

            // Call closure - this callback will return TransactionHaltException if
            // user has already committed the transaction
            $callback($queryTransaction);

            // If no errors have been thrown or the transaction wasn't completed within
            // the closure, commit the changes
            if ($this->db()->inTransaction() === true) {
                $this->db()->commit();
            }
        } catch (Throwable $e) {
            // Something went wrong. Rollback and throw DatabaseQueryBuilderError
            if ($this->db()->inTransaction() === true) {
                $this->db()->rollBack();
            }

            throw new QueryBuilderException($e->getMessage(), $e->getCode(), $e, $this->getQuery());
        }

        return $queryTransaction;
    }

    /**
     * @param string|Raw|Closure $table
     * @param string|string[]    $fields
     *
     * @return static
     */
    public function joinUsing($table, $fields, string $joinType = ''): self
    {
        if (is_array($fields) === false) {
            $fields = [$fields];
        }

        $joinBuilder = new JoinBuilder($this->connection);
        $joinBuilder->using($fields);

        $table = $this->addTablePrefix($table, false);

        $this->removeExistingStatement('joins', 'table', $table);

        $this->statements['joins'][] = [
            'type'        => $joinType,
            'table'       => $table,
            'joinBuilder' => $joinBuilder,
        ];

        return $this;
    }

    /**
     * Adds new LEFT JOIN statement to the current query.
     *
     * @param string|Raw|Closure|string[]    $table
     * @param string|JoinBuilder|Raw|Closure $key
     * @param string|Raw|Closure|null        $value
     *
     * @return static
     */
    public function leftJoin($table, $key, ?string $operator = null, $value = null): self
    {
        return $this->join($table, $key, $operator, $value, 'left');
    }

    public function skip(int $offset): self
    {
        return $this->offset($offset);
    }

    /**
     * Adds OFFSET statement to the current query.
     *
     * @return static $this
     */
    public function offset(int $offset): self
    {
        $this->statements['offset'] = $offset;

        return $this;
    }

    /**
     * Add on duplicate key statement.
     *
     * @param array $data
     *
     * @return static
     */
    public function onDuplicateKeyUpdate(array $data): self
    {
        $this->addStatement('onduplicate', $data);

        return $this;
    }

    /**
     * Adds OR HAVING statement to the current query.
     *
     * @param string|Raw|Closure     $key
     * @param string|Raw|Closure     $operator
     * @param mixed|Raw|Closure|null $value
     *
     * @return static
     */
    public function orHaving($key, $operator, $value): self
    {
        return $this->having($key, $operator, $value, 'OR');
    }

    /**
     * Adds HAVING statement to the current query.
     *
     * @param string|Raw|Closure $key
     * @param string|mixed       $operator
     * @param string|mixed       $value
     *
     * @return static
     */
    public function having($key, $operator = null, $value = null, string $joiner = 'AND'): self
    {
        $key = $this->addTablePrefix($key);
        $this->removeExistingStatement('havings', 'key', $key);
        $this->statements['havings'][] = compact('key', 'operator', 'value', 'joiner');

        return $this;
    }

    /**
     * @param array<string|int|float> $bindings
     *
     * @return static
     */
    public function havingRaw(string $sql, array $bindings = []): self
    {
        return $this->having($this->raw($sql, $bindings));
    }

    /**
     * Adds OR WHERE statement to the current query.
     *
     * @param string|Raw|Closure            $key
     * @param string|mixed|Raw|Closure|null $operator
     * @param mixed|Raw|Closure|null        $value
     *
     * @return static
     */
    public function orWhere($key, $operator = null, $value = null): self
    {
        // If two params are given then assume operator is =
        if (func_num_args() === 2) {
            $value    = $operator;
            $operator = '=';
        }

        return $this->whereHandler($key, $operator, $value, 'OR');
    }

    /**
     * Adds OR WHERE BETWEEN statement to the current query.
     *
     * @param string|Raw|Closure $key
     * @param string|int|float   $valueFrom
     * @param string|int|float   $valueTo
     *
     * @return static
     */
    public function orWhereBetween($key, $valueFrom, $valueTo): self
    {
        return $this->whereHandler($key, 'BETWEEN', [$valueFrom, $valueTo], 'OR');
    }

    /**
     * Adds OR WHERE IN statement to the current query.
     *
     * @param string|Raw|Closure $key
     * @param array|Raw|Closure  $values
     *
     * @return static
     */
    public function orWhereIn($key, $values): self
    {
        return $this->whereHandler($key, 'IN', $values, 'OR');
    }

    /**
     * Adds OR WHERE NOT statement to the current query.
     *
     * @param string|Raw|Closure            $key
     * @param string|mixed|Raw|Closure|null $operator
     * @param mixed|Raw|Closure|null        $value
     *
     * @return static
     */
    public function orWhereNot($key, $operator = null, $value = null): self
    {
        // If two params are given then assume operator is =
        if (func_num_args() === 2) {
            $value    = $operator;
            $operator = '=';
        }

        return $this->whereHandler($key, $operator, $value, 'OR NOT');
    }

    /**
     * Adds or WHERE NOT IN statement to the current query.
     *
     * @param string|Raw|Closure $key
     * @param array|Raw|Closure  $values
     *
     * @return static
     */
    public function orWhereNotIn($key, $values): self
    {
        return $this->whereHandler($key, 'NOT IN', $values, 'OR');
    }

    /**
     * Adds OR WHERE NOT NULL statement to the current query.
     *
     * @param string|Raw|Closure $key
     *
     * @return static
     */
    public function orWhereNotNull($key): self
    {
        return $this->whereNullHandler($key, 'NOT', 'or');
    }

    /**
     * Handles WHERE NULL statements.
     *
     * @param string|Raw|Closure $key
     *
     * @return static
     */
    protected function whereNullHandler($key, string $prefix = '', string $operator = ''): self
    {
        $this->removeExistingStatement('wheres', 'key', $key);

        $prefix = 'IS' . ($prefix !== '' ? ' ' . $prefix : '');

        return $this->{$operator . 'Where'}($key, $prefix, $this->raw('NULL'));
    }

    /**
     * Adds OR WHERE NULL statement to the current query.
     *
     * @param string|Raw|Closure $key
     *
     * @return static
     */
    public function orWhereNull($key): self
    {
        return $this->whereNullHandler($key, '', 'or');
    }

    /**
     * Performs query.
     *
     * @param array<string, mixed> $bindings
     *
     * @return static
     */
    public function query(string $sql, array $bindings = []): self
    {
        $queryObject = new QueryObject($sql, $bindings, $this->getConnection());
        $this->connection->setLastQuery($queryObject);

        $this->fireEvents(self::ACTION_BEFORE_QUERY, $queryObject);

        $executionTime = 0.0;
        $startTime     = microtime(true);

        $sql = $this->statement($queryObject->getSql(), $queryObject->getBindings());

        $this->db()->query($sql);

        $executionTime += microtime(true) - $startTime;

        $this->fireEvents(self::ACTION_AFTER_QUERY, $queryObject, ['execution_time' => $executionTime]);

        return $this;
    }

    /**
     * Replace key/value array
     *
     * @param array $data
     *
     * @return array|int|null
     */
    public function replace(array $data)
    {
        return $this->doInsert($data, 'replace');
    }

    /**
     * Adds new right join statement to the current query.
     *
     * @param string|Raw|Closure|array       $table
     * @param string|JoinBuilder|Raw|Closure $key
     * @param string|Raw|Closure|null        $value
     *
     * @return static
     */
    public function rightJoin($table, $key, ?string $operator = null, $value = null): self
    {
        return $this->join($table, $key, $operator, $value, 'right');
    }

    /**
     * Performs select distinct on the current query.
     *
     * @param string|Raw|Closure|array $fields
     *
     * @return static
     */
    public function selectDistinct($fields): self
    {
        if ($this->overwriteEnabled === true) {
            $this->statements['distincts'] = $fields;
        } else {
            $this->addStatement('distincts', $fields);
        }

        return $this;
    }

    /**
     * Add union
     *
     * @return static $this
     */
    public function union(QueryBuilder $query, ?string $unionType = self::UNION_TYPE_NONE): self
    {
        $statements = $query->getStatements();

        if (count($statements['unions']) > 0) {
            $this->statements['unions'] = $statements['unions'];
            unset($statements['unions']);
            $query->setStatements($statements);
        }

        $this->statements['unions'][] = [
            'query' => $query,
            'type'  => $unionType,
        ];

        return $this;
    }

    /**
     * Update or insert key/value array
     *
     * @param array $data
     *
     * @return array|bool|int|null
     */
    public function updateOrInsert(array $data)
    {
        if ($this->row() !== null) {
            return $this->update($data);
        }

        return $this->insert($data);
    }

    /**
     * Update key/value array
     *
     * @param array $data
     *
     * @return int|bool
     */
    public function update(array $data)
    {
        $queryObject = $this->getQuery('update', $data);

        $this->connection->setLastQuery($queryObject);

        $this->fireEvents(self::ACTION_BEFORE_UPDATE, $queryObject);

        $executionTime = 0.0;
        $startTime     = microtime(true);

        $sql      = $this->statement($queryObject->getSql(), $queryObject->getBindings());
        $response = $this->db()->query($sql);

        $executionTime += microtime(true) - $startTime;

        $this->fireEvents(self::ACTION_AFTER_UPDATE, $queryObject, ['execution_time' => $executionTime]);

        return $response;
    }

    /**
     * Insert key/value array
     *
     * @param array $data
     *
     * @return array|int|null
     */
    public function insert(array $data)
    {
        return $this->doInsert($data, 'insert');
    }

    /** @return static */
    public function whereDateBetween(string $column, string $startDate, string $endDate): self
    {
        $stat_date = $this->db()->getVar($this->db()->prepare('SELECT CAST(%s as DATE)', $startDate));
        $endDate   = $this->db()->getVar($this->db()->prepare('SELECT CAST(%s as DATE)', $endDate));

        return $this->whereBetween($column, $stat_date, $endDate);
    }

    public function whereExists(callable $callback): self
    {
        $query = $this->getConnection()->createQueryBuilder();
        call_user_func($callback, $query);

        return $this->whereHandler($this->raw(
            'EXISTS (' . $query->getQuery()->getSql() . ')',
            $query->getQuery()->getBindings()
        ));
    }

    /** @param mixed[]|string $column1 */
    public function whereColumn($column1, ?string $operator = null, ?string $column2 = null): self
    {
        if (is_array($column1)) {
            foreach ($column1 as $column) {
                $this->whereColumn(...$column);
            }

            return $this;
        }

        if (func_num_args() === 2) {
            $column2  = $operator;
            $operator = '=';
        }

        return $this->whereHandler($this->raw(sprintf(
            '%s %s %s',
            $this->adapter->wrapSanitizer($column1),
            $operator,
            $this->adapter->wrapSanitizer($column2)
        )));
    }

    public function whereYear(string $column, string $operator, ?string $year = null): self
    {
        if (func_num_args() === 2) {
            $year     = $operator;
            $operator = '=';
        }

        $this->whereHandler($this->raw(
            sprintf('YEAR(%s) %s ?', $this->adapter->wrapSanitizer($column), $operator),
            $year
        ));

        return $this;
    }

    public function whereDate(string $column, string $operator, ?string $date = null): self
    {
        if (func_num_args() === 2) {
            $date     = $operator;
            $operator = '=';
        }

        $this->whereHandler($this->raw(sprintf(
            'DATE(%s) %s ?',
            $this->adapter->wrapSanitizer($column),
            $operator
        ), $date));

        return $this;
    }

    public function whereMonth(string $column, string $operator, ?string $month = null): self
    {
        if (func_num_args() === 2) {
            $month    = $operator;
            $operator = '=';
        }

        $this->whereHandler($this->raw(sprintf(
            'MONTH(%s) %s ?',
            $this->adapter->wrapSanitizer($column),
            $operator
        ), $month));

        return $this;
    }

    public function whereDay(string $column, string $operator, ?string $day = null): self
    {
        if (func_num_args() === 2) {
            $day      = $operator;
            $operator = '=';
        }

        $this->whereHandler($this->raw(sprintf(
            'DAY(%s) %s ?',
            $this->adapter->wrapSanitizer($column),
            $operator
        ), $day));

        return $this;
    }

    public function whereTime(string $column, string $operator, ?string $time = null): self
    {
        if (func_num_args() === 2) {
            $time     = $operator;
            $operator = '=';
        }

        $this->whereHandler($this->raw(sprintf(
            'TIME(%s) %s ?',
            $this->adapter->wrapSanitizer($column),
            $operator
        ), $time));

        return $this;
    }

    /**
     * Adds WHERE BETWEEN statement to the current query.
     *
     * @param string|Raw|Closure           $key
     * @param string|int|float|Raw|Closure $valueFrom
     * @param string|int|float|Raw|Closure $valueTo
     *
     * @return static
     */
    public function whereBetween($key, $valueFrom, $valueTo): self
    {
        return $this->whereHandler($key, 'BETWEEN', [$valueFrom, $valueTo]);
    }

    public function whereDateNotBetween(string $column, string $startDate, string $endDate): self
    {
        $startDate = $this->db()->getVar($this->db()->prepare('SELECT CAST(%s as DATE)', $startDate));
        $endDate   = $this->db()->getVar($this->db()->prepare('SELECT CAST(%s as DATE)', $endDate));

        return $this->whereNotBetween($column, $startDate, $endDate);
    }

    public function whereNotBetween($key, $valueFrom, $valueTo): self
    {
        return $this->whereHandler($key, 'NOT BETWEEN', [$valueFrom, $valueTo]);
    }

    /**
     * Adds WHERE IN statement to the current query.
     *
     * @param string|Raw|Closure  $key
     * @param mixed[]|Raw|Closure $values
     *
     * @return static
     */
    public function whereIn($key, $values): self
    {
        return $this->whereHandler($key, 'IN', $values);
    }

    /**
     * Adds WHERE NOT statement to the current query.
     *
     * @param string|Raw|Closure              $key
     * @param string|mixed[]|Raw|Closure|null $operator
     * @param mixed|Raw|Closure|null          $value
     *
     * @return static
     */
    public function whereNot($key, $operator = null, $value = null): self
    {
        // If two params are given then assume operator is =
        if (func_num_args() === 2) {
            $value    = $operator;
            $operator = '=';
        }

        return $this->whereHandler($key, $operator, $value, 'AND NOT');
    }

    /**
     * Adds OR WHERE NOT IN statement to the current query.
     *
     * @param string|Raw|Closure  $key
     * @param mixed[]|Raw|Closure $values
     *
     * @return static
     */
    public function whereNotIn($key, $values): self
    {
        return $this->whereHandler($key, 'NOT IN', $values);
    }

    /**
     * Adds WHERE NOT NULL statement to the current query.
     *
     * @param string|Raw|Closure $key
     *
     * @return static
     */
    public function whereNotNull($key): self
    {
        return $this->whereNullHandler($key, 'NOT');
    }

    /**
     * Adds WHERE NULL statement to the current query.
     *
     * @param string|Raw|Closure $key
     *
     * @return static
     */
    public function whereNull($key): self
    {
        return $this->whereNullHandler($key);
    }

    public function whereNotLike(string $key, string $value): self
    {
        return $this->whereHandler($key, 'NOT LIKE', $value);
    }

    public function whereStartsWith(string $key, string $value): self
    {
        return $this->whereLike($key, $value . '%');
    }

    public function whereLike(string $key, string $value): self
    {
        return $this->whereHandler($key, 'LIKE', $value);
    }

    public function search(string $key, ?string $value = null): self
    {
        return $this->whereLike($key, '%' . $value . '%');
    }

    /**
     * Will add FOR statement to the end of the SELECT statement, like FOR UPDATE, FOR SHARE etc.
     *
     * @return static
     */
    public function for(string $statement): self
    {
        $this->addStatement('for', $statement);

        return $this;
    }

    /**
     * Returns all columns in current query
     *
     * @return array<string, mixed>
     */
    public function getColumns(): array
    {
        $tSelects = isset($this->statements['selects']) === true ? $this->statements['selects'] : [];
        $tColumns = [];
        foreach ($tSelects as $key => $value) {
            if (! is_string($value)) {
                continue;
            }

            if (is_int($key)) {
                $tElements = explode('.', $value);
                if (! in_array('*', $tElements, true)) {
                    $tColumns[$tElements[1] ?? $tElements[0]] = $value;
                }
            } elseif (is_string($key)) {
                $tColumns[$value] = $key;
            }
        }

        return $tColumns;
    }

    /**
     * Returns boolean value indicating if overwriting is enabled or disabled in QueryBuilderHandler.
     */
    public function isOverwriteEnabled(): bool
    {
        return $this->overwriteEnabled;
    }

    /**
     * If enabled calling from, select etc. will overwrite any existing values from previous calls in query.
     *
     * @return static
     */
    public function setOverwriteEnabled(bool $enabled = true): self
    {
        $this->overwriteEnabled = $enabled;

        return $this;
    }

    public function __destruct()
    {
        $this->close();
    }

    /**
     * Close connection
     */
    public function close(): void
    {
        $this->connection = null;
    }

    protected function getQueryAdapter(): IQueryAdapter
    {
        return $this->adapter;
    }
}
