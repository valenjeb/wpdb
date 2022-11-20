<?php

declare(strict_types=1);

namespace Devly\WPDB\Query;

use Devly\Exceptions\DatabaseException;
use wpdb;

use function implode;
use function in_array;
use function sprintf;

class Table
{
    /**
     * Create a table
     *
     * @param string[]|Column[] $columns
     */
    public static function create(string $tableName, array $columns, bool $tablePrefix = true): bool
    {
        $tableName = self::prefixTableName($tableName, $tablePrefix);

        self::ensureColumnsNotEmpty($columns, $tableName);

        /** @noinspection SqlNoDataSourceInspection */
        $sql = sprintf('CREATE TABLE `%s` (%s)', $tableName, self::buildColumnsQuery($columns));

        self::query($sql);

        return self::exists($tableName);
    }

    /**
     * Create table if not exists
     *
     * @param string            $name    The table name to create
     * @param string[]|Column[] $columns
     * @param bool              $prefix  Whether to prefix the table name
     */
    public static function createIfNotExists(string $name, array $columns, bool $prefix = true): bool
    {
        $name = self::prefixTableName($name, $prefix);

        self::ensureColumnsNotEmpty($columns, $name);

        /** @noinspection SqlNoDataSourceInspection */
        $sql = sprintf('CREATE TABLE IF NOT EXISTS `%s` (%s)', $name, self::buildColumnsQuery($columns));

        self::query($sql);

        return self::exists($name);
    }

    /** @param string[]|Column[] $columns */
    protected static function buildColumnsQuery(array $columns): string
    {
        $primaryKey = [];
        $unique     = [];
        $index      = [];

        foreach ($columns as $column) {
            if (! $column instanceof Column) {
                continue;
            }

            if ($column->isPrimaryKey()) {
                $primaryKey[] = self::wrap($column->getName());
                continue;
            }

            if ($column->isIndex()) {
                $unique[] = self::wrap($column->getName());
                continue;
            }

            if (! $column->isUnique()) {
                continue;
            }

            $index[] = self::wrap($column->getName());
        }

        if (! empty($primaryKey)) {
            $columns[] = sprintf('PRIMARY KEY (%s)', implode(', ', $primaryKey));
        }

        if (! empty($index)) {
            $columns[] = sprintf('INDEX (%s)', implode(', ', $index));
        }

        if (! empty($unique)) {
            $columns[] = sprintf('UNIQUE (%s)', implode(', ', $primaryKey));
        }

        return implode(' , ', $columns);
    }

    protected static function wrap(string $name): string
    {
        return sprintf('`%s`', $name);
    }

    /**
     * Performs a database query, using current database connection.
     */
    protected static function query(string $sql): void
    {
        $wpdb = self::wpdb();
        $wpdb->query($sql);

        if (($wpdb->last_error ?: null) === null) {
            return;
        }

        throw new DatabaseException($wpdb->last_error . '. SQL query: ' . $sql);
    }

    /** @param string[]|Column[] $columns */
    protected static function ensureColumnsNotEmpty(array $columns, string $tableName): void
    {
        if (! empty($columns)) {
            return;
        }

        throw new DatabaseException(sprintf(
            'Failed to create table "%s" Table must contain at least 1 column.',
            $tableName
        ));
    }

    protected static function prefixTableName(string $name, bool $table): string
    {
        if (! $table) {
            return $name;
        }

        return self::wpdb()->prefix . $name;
    }

    /**
     * Checks whether a table name exists
     */
    public static function exists(string $name): bool
    {
        $tables = self::wpdb()->get_col('SHOW TABLES');

        return in_array($name, $tables);
    }

    protected static function wpdb(): wpdb
    {
        return $GLOBALS['wpdb'];
    }
}
