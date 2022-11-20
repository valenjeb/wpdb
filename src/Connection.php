<?php

declare(strict_types=1);

namespace Devly\WPDB;

use Devly\WPDB\Contracts\IQueryAdapter;
use Devly\WPDB\Query\Adapters\Mysql;
use Devly\WPDB\Query\QueryBuilder;
use Devly\WPDB\Query\QueryObject;

use function is_string;

class Connection
{
    protected static ?Connection $storedConnection;

    protected IQueryAdapter $adapter;

    protected ?QueryObject $lastQuery;

    private DatabaseConnection $connection;

    protected ?string $tablePrefix = null;

    /** @param string|IQueryAdapter|null $adapter Adapter name or class */
    public function __construct($adapter = null, ?DatabaseConnection $connection = null)
    {
        $this->connection = $connection ?? $this->createDatabaseConnection();

        if ($adapter === null) {
            $adapter = Mysql::class;
        }

        if (is_string($adapter)) {
            $adapter = new $adapter($this);
        }

        $this->setQueryAdapter($adapter);

        if (! empty(static::$storedConnection)) {
            return;
        }

        static::$storedConnection = $this;
    }

    public static function getStoredConnection(): ?self
    {
        return static::$storedConnection;
    }

    public function getQueryAdapter(): IQueryAdapter
    {
        return $this->adapter;
    }

    public function getDatabaseConnection(): DatabaseConnection
    {
        return $this->connection;
    }

    /**
     * Returns an instance of Query Builder
     */
    public function createQueryBuilder(): QueryBuilder
    {
        return new QueryBuilder($this);
    }

    public function setQueryAdapter(IQueryAdapter $adapter): self
    {
        $this->adapter = $adapter;

        return $this;
    }

    /**
     * Set query-object for last executed query.
     *
     * @return static
     */
    public function setLastQuery(QueryObject $query): self
    {
        $this->lastQuery = $query;

        return $this;
    }

    /**
     * Get query-object from last executed query.
     */
    public function getLastQuery(): ?QueryObject
    {
        return $this->lastQuery;
    }

    /**
     * Close PDO connection
     */
    public function close(): void
    {
        static::$storedConnection = null;
    }

    public function __destruct()
    {
        $this->close();
    }

    /**
     * @param string|bool $prefix The table prefix, true to use the default WP
     *                            table prefix as set in wp-config.php or false
     *                            to skip prefixing.
     */
    public function setTablePrefix($prefix = true): Connection
    {
        if ($prefix === false) {
            return $this;
        }

        if (is_string($prefix)) {
            $this->getDatabaseConnection()->setPrefix($prefix);
        }

        $this->tablePrefix = $this->getDatabaseConnection()->getPrefix();

        return $this;
    }

    public function getTablePrefix(): ?string
    {
        return $this->tablePrefix;
    }

    protected function createDatabaseConnection(): DatabaseConnection
    {
        return new DatabaseConnection();
    }
}
