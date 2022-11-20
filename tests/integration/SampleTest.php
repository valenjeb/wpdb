<?php

declare(strict_types=1);

namespace Devly\WPDB\Tests;

use Devly\WPDB\DB;
use WP_UnitTestCase;

class SampleTest extends WP_UnitTestCase
{
    public function testDatabaseQueryBuilder(): void
    {
        $q = DB::table('wptests_posts')->getQuery()->getSql();
        $this->assertEquals('SELECT * FROM wptests_posts', $q);
    }

    public function testSelectWhere(): void
    {
        $q = DB::table('posts')->select('post_title')->where('ID', 1)->getQuery()->getRawSql();
        $this->assertEquals('SELECT post_title FROM posts WHERE ID = 1', $q);
    }
}
