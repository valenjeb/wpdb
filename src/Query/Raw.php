<?php

declare(strict_types=1);

namespace Devly\WPDB\Query;

class Raw
{
    protected string $value;

    /** @var array<string|int|float> */
    protected array $bindings;

    /** @param array<string|int|float>|string $bindings */
    public function __construct(string $value, $bindings = [])
    {
        $this->value    = $value;
        $this->bindings = (array) $bindings;
    }

    public function __toString(): string
    {
        return $this->value;
    }

    /** @return array|float[]|int[]|string[] */
    public function getBindings(): array
    {
        return $this->bindings;
    }
}
