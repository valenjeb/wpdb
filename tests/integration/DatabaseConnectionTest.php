<?php

declare(strict_types=1);

namespace Devly\WPDB\Tests;

use Devly\WPDB\DatabaseConnection;
use PHPUnit\Framework\TestCase;

class DatabaseConnectionTest extends TestCase
{
    public function testDatabaseConnection(): void
    {
        $connection = new DatabaseConnection();

        $this->assertInstanceOf('wpdb', $connection->wpdb());
    }

    public function testPrepareStatement(): void
    {
        $connection = new DatabaseConnection();

        $stmt = $connection->prepare('SELECT * FROM `users` WHERE `user_status` = %s', 'published');
        $this->assertEquals("SELECT * FROM `users` WHERE `user_status` = 'published'", $stmt);
    }
}
