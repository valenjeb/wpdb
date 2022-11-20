<?php

declare(strict_types=1);

namespace Devly\WPDB\Query;

use Closure;

class JoinBuilder extends QueryBuilder
{
    /**
     * Add join
     *
     * @param string|Raw|Closure $key
     * @param string|Raw|Closure $operator
     * @param string|Raw|Closure $value
     */
    public function on($key, $operator, $value, string $joiner = 'AND'): self
    {
        $this->statements['criteria'][] = [
            'key'       => $this->addTablePrefix($key),
            'operator'  => $operator,
            'value'     => $this->addTablePrefix($value),
            'joiner'    => $joiner,
            'condition' => 'ON',
        ];

        return $this;
    }

    /**
     * Add join with USING syntax
     *
     * @param string[] $columns
     */
    public function using(array $columns): self
    {
        $this->statements['criteria'][] = [
            'columns' => $this->addTablePrefix($columns),
            'joiner'  => 'AND USING',
        ];

        return $this;
    }

    /**
     * Add OR ON join
     *
     * @param string|Raw|Closure $key
     * @param string|Raw|Closure $operator
     * @param string|Raw|Closure $value
     */
    public function orOn($key, $operator, $value): self
    {
        return $this->on($key, $operator, $value, 'OR');
    }
}
