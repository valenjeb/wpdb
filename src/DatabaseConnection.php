<?php

declare(strict_types=1);

namespace Devly\WPDB;

use BadMethodCallException;
use Devly\Exceptions\DatabaseException;
use InvalidArgumentException;
use stdClass;
use WP_Error;
use wpdb;

use function call_user_func_array;
use function implode;
use function in_array;
use function lcfirst;
use function method_exists;
use function preg_match_all;
use function property_exists;
use function sprintf;
use function strpos;
use function strtolower;
use function strtoupper;
use function trigger_error;
use function trim;

use const ARRAY_A;
use const ARRAY_N;
use const E_USER_WARNING;
use const OBJECT_K;

/**
 * @property string $users WordPress Users table name.
 * @property string $usermeta WordPress user metadata table name.
 * @property string $posts WordPress Posts table name.
 * @property string $postmeta WordPress post metadata (a.k.a. Custom Fields) table name.
 * @property string $comments WordPress post comments table name.
 * @property string $commentate WordPress post comment metadata table name.
 * @property string $links WordPress Links table name.
 * @property string $termmeta WordPress Term Meta table name.
 * @property string $terms WordPress Terms table name.
 * @property string $term_taxonomy WordPress Term Taxonomy table name.
 * @property string $term_relationships WordPress Term Relationships table name.
 * @property string $options WordPress Options table name.
 * @property string $blogs Multisite Blogs' table name.
 * @property string $blogmeta Multisite Blog Metadata table name.
 * @property string $signups Multisite Signups table name.
 * @property string $site Multisite Sites table name.
 * @property string $sitemeta Multisite Site Metadata table name.
 * @property string $sitecategories Multisite Site-wide Terms table name.
 * @property string $registration_log Multisite Registration Log table name.
 * @property string $blog_versions
 */
class DatabaseConnection
{
    protected wpdb $wpdb;

    private bool $inTransaction = false;

    public function __construct(?wpdb $wpdb = null)
    {
        $this->wpdb = $wpdb ?? $GLOBALS['wpdb'];

        $this->wpdb()->hide_errors();
    }

    /**
     * @param array<int, mixed> $arguments
     *
     * @return mixed
     */
    public function __call(string $method, array $arguments)
    {
        $_method = $this->fromCamelCase($method);
        if (method_exists($this->wpdb(), $_method)) {
            return call_user_func_array([$this->wpdb(), $_method], $arguments);
        }

        throw new BadMethodCallException(sprintf('Method "%s" does not exist.', $method));
    }

    /** @return mixed */
    public function __get(string $name)
    {
        if (property_exists($this->wpdb(), $name)) {
            return $this->wpdb()->$name;
        }

        trigger_error(
            sprintf('Undefined property %s::$%s.', self::class, $name),
            E_USER_WARNING
        );

        return null;
    }

    /**
     * Retrieves the ID generated for an AUTO_INCREMENT column
     * by the last query (usually INSERT).
     */
    public function lastInsertId(): int
    {
        return $this->wpdb()->insert_id;
    }

    /**
     * Retrieve results of the last query.
     *
     * @return array<stdClass>|null
     */
    public function lastResult(): ?array
    {
        return $this->wpdb()->last_result;
    }

    /**
     * Retrieve the WordPress table prefix.
     */
    public function getPrefix(): string
    {
        return $this->wpdb()->prefix;
    }

    /**
     * Sets the table prefix for WordPress tables.
     *
     * @throws DatabaseException if error is encountered during execution.
     */
    public function setPrefix(string $prefix, bool $tableNames = true): string
    {
        $result = $this->wpdb()->set_prefix($prefix, $tableNames);

        if ($result instanceof WP_Error) {
            throw new DatabaseException($result->get_error_message());
        }

        return $result;
    }

    /**
     * Retrieves an entire SQL result set from the database
     *
     * @param string|null $query  SQL query
     * @param string      $output Any of ARRAY_A | ARRAY_N | OBJECT | OBJECT_K constants.
     *                            With one of the first three, return an array of rows
     *                            indexed from 0 by SQL result row number. Each row is an
     *                            associative array (column => value, ...), a numerically
     *                            indexed array (0 => value, ...), or an object ( ->column = value ),
     *                            respectively. With OBJECT_K, return an associative array
     *                            of row objects keyed by the value of each row's first column's
     *                            value. Duplicate keys are discarded.
     *
     * @return array<array<string,mixed>|stdClass>|object|null
     */
    public function getResults(?string $query = null, string $output = OBJECT)
    {
        $supported = [OBJECT, OBJECT_K, ARRAY_A, ARRAY_N];

        if (! in_array($output, $supported)) {
            throw new InvalidArgumentException(sprintf('Output type must be one of: %s.', implode(', ', $supported)));
        }

        $results = $this->wpdb()->get_results($query, $output);
        $error   = $this->lastErrorMessage();
        if (! empty($error)) {
            throw new DatabaseException($error);
        }

        return $results;
    }

    /**
     * Retrieves one row from the database.
     *
     * @param string|null $query  SQL query
     * @param string      $output The required return type. One of OBJECT, ARRAY_A,
     *                            or ARRAY_N, which correspond to an stdClass object,
     *                            an associative array, or a numeric array, respectively.
     * @param int         $y      Row to return. Indexed from 0.
     *
     * @return array<string, mixed>|object|null
     *
     * @throws DatabaseException if error occurred during execution.
     */
    public function getRow(?string $query = null, string $output = OBJECT, int $y = 0)
    {
        $supported = [OBJECT, ARRAY_A, ARRAY_N];

        if (! in_array($output, $supported)) {
            throw new InvalidArgumentException(sprintf('Output type must be one of: %s.', implode(', ', $supported)));
        }

        $result = $this->wpdb()->get_row($query, $output, $y);
        $error  = $this->lastErrorMessage();
        if (! empty($error)) {
            throw new DatabaseException($error);
        }

        return $result;
    }

    /**
     * Retrieves one variable from the database.
     *
     * Executes a SQL query and returns the value from the SQL result.
     *
     * @param string|null $query SQL query. Defaults to null, use the result
     *                           from the previous query.
     * @param int         $x     Column of value to return. Indexed from 0.
     * @param int         $y     Row of value to return. Indexed from 0.
     *
     * @throws DatabaseException if error occurred during execution.
     */
    public function getVar(?string $query = null, int $x = 0, int $y = 0): ?string
    {
        $result = $this->wpdb()->get_var($query, $x, $y);
        $error  = $this->lastErrorMessage();
        if (! empty($error)) {
            throw new DatabaseException($error);
        }

        return $result;
    }

    /**
     * Retrieves one column from the database.
     *
     * Executes a SQL query and returns the column from the SQL result.
     * If the SQL result contains more than one column, the column specified
     * is returned. If $query is null, the specified column from the previous
     * SQL result is returned.
     *
     * @param string|null $query SQL query. Defaults to previous query.
     * @param int         $x     Column to return. Indexed from 0.
     *
     * @return array<string, mixed>
     *
     * @throws DatabaseException if error occurred during execution.
     */
    public function getCol(?string $query = null, int $x = 0): array
    {
        $result = $this->wpdb()->get_col($query, $x);
        $error  = $this->lastErrorMessage();
        if (! empty($error)) {
            throw new DatabaseException($error);
        }

        return $result;
    }

    /**
     * Retrieves the character set for the given column.
     *
     * @return false|string
     *
     * @throws DatabaseException if error occurred during execution.
     */
    public function getColCharset(string $table, string $column)
    {
        $result = $this->wpdb()->get_col_charset($table, $column);

        if ($result instanceof WP_Error) {
            throw new DatabaseException($result->get_error_message());
        }

        return $result;
    }

    /**
     * Gets blog prefix.
     */
    public function getBlogPrefix(?int $blogID = null): string
    {
        return $this->wpdb()->get_blog_prefix($blogID);
    }

    /**
     * Performs a MySQL database query, using current database connection.
     *
     * @return bool|int Boolean true for CREATE, ALTER, TRUNCATE and DROP
     *                  queries. Number of rows affected/selected for all
     *                  other queries.
     *
     * @throws DatabaseException if error occurred during execution.
     */
    public function query(string $query)
    {
        $result = $this->wpdb()->query($query);
        $error  = $this->lastErrorMessage();

        if (! empty($error)) {
            throw new DatabaseException($error);
        }

        return $result;
    }

    /**
     * The error encountered during the last query.
     */
    public function lastErrorMessage(): ?string
    {
        $message = $this->wpdb()->last_error;

        return ! empty(trim($message)) ? $message : null;
    }

    /**
     * Prepares a SQL query for safe execution.
     *
     * Uses sprintf()-like syntax. The following placeholders can be used in the
     * query string:
     *  - %d (integer)
     *  - %f (float)
     *  - %s (string)
     *
     * All placeholders MUST be left unquoted in the query string. A corresponding
     * argument MUST be passed for each placeholder.
     *
     * Arguments may be passed as individual arguments to the method, or as a single
     * array containing all arguments. A combination of the two is not supported.
     *
     * @param string $query   Query statement with sprintf()-like placeholders.
     * @param mixed  ...$args The array of variables to substitute into the
     *                        query's placeholders if being called with an array
     *                        of arguments, or the first variable to substitute
     *                        into the query's placeholders if being called with
     *                        individual arguments.
     *
     * @return string Sanitized query string, if there is a query to prepare.
     */
    public function prepare(string $query, ...$args): string
    {
        if (strpos($query, '%') === false) {
            throw new DatabaseException(sprintf(
                'The query argument of %s::prepare() must have a placeholder.',
                self::class
            ));
        }

        return $this->wpdb()->prepare($query, ...$args);
    }

    public function inTransaction(): bool
    {
        return $this->inTransaction;
    }

    public function beginTransaction(): self
    {
        $this->query('START TRANSACTION');

        $this->inTransaction = true;

        return $this;
    }

    public function commit(): self
    {
        $this->inTransaction = false;

        $this->query('COMMIT');

        return $this;
    }

    public function rollback(): self
    {
        $this->inTransaction = false;

        $this->query('ROLLBACK');

        return $this;
    }

    protected function fromCamelCase(string $input): string
    {
        $pattern = '!([A-Z][A-Z0-9]*(?=$|[A-Z][a-z0-9])|[A-Za-z][a-z0-9]+)!';
        preg_match_all($pattern, $input, $matches);
        $ret = $matches[0];
        foreach ($ret as &$match) {
            $match = $match === strtoupper($match) ?
                strtolower($match) :
                lcfirst($match);
        }

        return implode('_', $ret);
    }

    public function wpdb(): wpdb
    {
        return $this->wpdb;
    }
}
