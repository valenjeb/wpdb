<?php

declare(strict_types=1);

/**
 * Plugin Name:     WP Database
 * Plugin URI:      https://github.com/valenjeb/wpdb
 * Description:     PLUGIN DESCRIPTION HERE
 * Author:          Valentin Jebelev
 * Author URI:      https://github.com/valenjeb
 * Text Domain:     wpdb
 * Domain Path:     /languages
 * Version:         0.1.0
 */

$autoload = dirname(__FILE__) . '/vendor/autoload.php';

if (! file_exists($autoload)) {
    wp_die();
}

require_once $autoload;
