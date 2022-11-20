<?php

declare(strict_types=1);

namespace Devly\WPDB;

use Devly\Exceptions\DatabaseException;
use Devly\WPDB\Query\Column;
use Devly\WPDB\Query\QueryBuilder;
use Devly\WPDB\Query\Raw;
use Devly\WPDB\Query\Table;

use function array_shift;
use function func_get_args;
use function implode;
use function in_array;
use function is_array;
use function sprintf;

/**
 * @method static QueryBuilder posts()
 * @method static QueryBuilder postmeta()
 * @method static QueryBuilder comments()
 * @method static QueryBuilder commentate()
 * @method static QueryBuilder termmeta()
 * @method static QueryBuilder terms()
 * @method static QueryBuilder term_taxonomy()
 * @method static QueryBuilder term_relationships()
 * @method static QueryBuilder users()
 * @method static QueryBuilder usermeta()
 * @method static QueryBuilder links()
 * @method static QueryBuilder options()
 * @method static QueryBuilder blogs()
 * @method static QueryBuilder blogmeta()
 * @method static QueryBuilder signups()
 * @method static QueryBuilder site()
 * @method static QueryBuilder sitemeta()
 * @method static QueryBuilder sitecategories()
 * @method static QueryBuilder registration_log()
 * @method static QueryBuilder blog_versions()
 */
class DB
{
    public const WP_TABLES = [
        'posts',
        'postmeta',
        'comments',
        'commentate',
        'termmeta',
        'terms',
        'term_taxonomy',
        'term_relationships',
        'users',
        'usermeta',
        'links',
        'options',
        'blogs',
        'blogmeta',
        'signups',
        'site',
        'sitemeta',
        'sitecategories',
        'registration_log',
        'blog_versions',
    ];

    /**
     * @param string|string[]|null $tables
     * @param bool|string          $prefix
     */
    public static function table($tables, $prefix = false): QueryBuilder
    {
        return self::createConnection()->setTablePrefix($prefix)->createQueryBuilder()->from($tables);
    }

    /**
     * Adds fields to select on the current query (defaults is all).
     *
     * You can use key/value array to create alias. Sub-queries and raw-objects are also supported.
     * Example: ['field' => 'alias'] will become field AS alias
     *
     * @param Raw|Raw[]|string|string[] $columns
     */
    public static function select($columns): QueryBuilder
    {
        $columns = is_array($columns) ? $columns : func_get_args();

        return self::createConnection()->createQueryBuilder()->select($columns);
    }

    /** @param mixed $arguments */
    public static function __callStatic(string $name, $arguments): QueryBuilder
    {
        return self::createForTable($name);
    }

    /**
     * Create QueryBuilder object for the provided table name.
     */
    protected static function createForTable(string $name): QueryBuilder
    {
        if (! in_array($name, self::WP_TABLES)) {
            $message = sprintf(
                'Table "%s" is not a WordPress table. Available tables are: %s.',
                $name,
                implode(', ', self::WP_TABLES)
            );

            throw new DatabaseException($message);
        }

        $connection = self::createConnection();
        $table      = $connection->getDatabaseConnection()->$name;

        return $connection->createQueryBuilder()->from($table);
    }

    /**
     * Create new Raw object
     *
     * @param array<string|float|int>|string $bindings
     */
    public static function raw(string $value, $bindings = null): Raw
    {
        if (is_array($bindings) === false) {
            $bindings = func_get_args(); // phpcs:ignore
            array_shift($bindings);
        }

        return new Raw($value, $bindings);
    }

    public static function createConnection(?string $adapter = null): Connection
    {
        return new Connection($adapter);
    }

    /**
     * Create table if not exists
     *
     * @param string            $name    The table name to create.
     * @param string[]|Column[] $columns List of table columns.
     * @param bool              $prefix  Whether to prefix the table name.
     *
     * @return bool Whether the table created successfully
     */
    public static function maybeCreateTable(string $name, array $columns, bool $prefix = true): bool
    {
        return Table::createIfNotExists($name, $columns, $prefix);
    }

    /**
     * Create table
     *
     * @param string            $name    The table name to create.
     * @param string[]|Column[] $columns List of table columns.
     * @param bool              $prefix  Whether to prefix the table name.
     *
     * @return bool Whether the table created successfully
     */
    public static function createTable(string $name, array $columns, bool $prefix = true): bool
    {
        return Table::create($name, $columns, $prefix);
    }
}
