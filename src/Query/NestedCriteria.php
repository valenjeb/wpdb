<?php

declare(strict_types=1);

namespace Devly\WPDB\Query;

use Closure;

use function compact;

class NestedCriteria extends QueryBuilder
{
    /**
     * @param string|Raw|Closure            $key
     * @param string|array|Raw|Closure|null $value
     */
    protected function whereHandler($key, ?string $operator = null, $value = null, string $joiner = 'AND'): QueryBuilder
    {
        $key = $this->addTablePrefix($key);

        $this->statements['criteria'][] = compact('key', 'operator', 'value', 'joiner');

        return $this;
    }
}
