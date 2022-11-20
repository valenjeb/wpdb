<?php

declare(strict_types=1);

namespace Devly\WPDB\Query;

use Exception;
use LogicException;

use function explode;
use function in_array;
use function sprintf;
use function strtoupper;

class Column
{
    protected const TEXT_TYPES           = ['CHAR', 'VARCHAR', 'TINYTEXT', 'TEXT', 'MEDIUMTEXT', 'LONGTEXT'];
    protected const VARIABLE_LENGTH_TYPE = ['VARCHAR', 'VARBINARY', 'BINARY', 'TEXT'];

    protected string $name;
    protected bool $primary_key = false;
    protected bool $unique      = false;
    protected string $type;
    protected bool $null_allowed   = false;
    protected bool $auto_increment = false;
    protected ?string $attributes  = null;
    protected ?string $collation   = null;
    protected ?int $length;
    protected bool $index = false;

    public function __construct(string $name, string $type, ?int $length = null)
    {
        $this->name = $name;
        $this->type = strtoupper($type);

        if ($length === null && in_array($this->type, self::VARIABLE_LENGTH_TYPE)) {
            throw new LogicException(sprintf(
                'The "%s" data type require to specify the length of the column.',
                $this->type
            ));
        }

        $this->length = $length;
    }

    /**
     * @throws Exception If length is not specified and data type is on of:
     *                   'VARCHAR', 'VARBINARY', 'BINARY', 'TEXT'.
     */
    public static function new(string $name, string $type, ?int $length = null): self
    {
        return new self($name, $type, $length);
    }

    public function __toString(): string
    {
        $string = sprintf(
            '`%s` %s',
            $this->name,
            $this->type . ($this->length !== null ? sprintf('(%d)', $this->length) : '')
        );

        if ($this->attributes) {
            $string .= sprintf(' %s', strtoupper($this->attributes));
        }

        if ($this->collation && in_array($this->type, self::TEXT_TYPES)) {
            $parts   = explode('_', $this->collation);
            $string .= sprintf(' CHARACTER SET %s COLLATE %s', $parts[0], $this->collation);
        }

        $string .= ($this->isNullAllowed() ? '' : ' NOT') . ' NULL';

        if ($this->auto_increment) {
            $string .= ' AUTO_INCREMENT';
        }

        return $string;
    }

    public function attributes(string $name): self
    {
        $this->attributes = $name;

        return $this;
    }

    public function unsignedZeroFill(): self
    {
        return $this->attributes('UNSIGNED ZEROFILL');
    }

    public function unsigned(): self
    {
        return $this->attributes('UNSIGNED');
    }

    public function binary(): self
    {
        return $this->attributes('BINARY');
    }

    public function onUpdateCurrentTimeStamp(): self
    {
        return $this->attributes('on update CURRENT_TIMESTAMP');
    }

    public function collate(string $name): self
    {
        $this->collation = $name;

        return $this;
    }

    public function alloweNull(): self
    {
        $this->null_allowed = true;

        return $this;
    }

    public function autoIncrement(): self
    {
        $this->auto_increment = true;

        return $this;
    }

    public function isUnique(): bool
    {
        return $this->unique;
    }

    public function unique(): self
    {
        $this->unique = true;

        return $this;
    }

    public function isIndex(): bool
    {
        return $this->index;
    }

    public function index(): self
    {
        $this->index = true;

        return $this;
    }

    public function isPrimaryKey(): bool
    {
        return $this->primary_key;
    }

    public function primaryKey(): self
    {
        $this->primary_key = true;

        return $this;
    }

    public function getName(): string
    {
        return $this->name;
    }

    protected function isNullAllowed(): bool
    {
        return $this->null_allowed;
    }
}
