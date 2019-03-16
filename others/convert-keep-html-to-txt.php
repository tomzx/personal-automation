<?php

use Symfony\Component\DomCrawler\Crawler;

require_once __DIR__ . '/../vendor/autoload.php';

function processFile(string $file) {
    $content = file_get_contents($file);
    $content = str_replace('<br>', PHP_EOL, $content);
    $dom = new Crawler($content);

    $date = date('Y-m-d H:i:s', strtotime($dom->filter('.heading')->text()));
    $archived = $dom->filter('.archived')->count() > 0;

    $body = $dom->filter('.content')->text();

    $tags = $dom->filter('.label-name')->each(function (Crawler $node, $i) {
        return '#' . $node->text();
    });

    return $date . ($archived ? ' ^archived' : '') . PHP_EOL . $body . PHP_EOL . implode(' ', $tags);
}

function processDirectory(string $directory, string $outputDirectory) {
    if (!file_exists($outputDirectory)) {
        mkdir($outputDirectory, 0700, true);
    }

    $files = glob($directory . '/*.html');
    foreach ($files as $file) {
        $content = processFile($file);
        $filename = basename($file, '.html');
        file_put_contents($outputDirectory . '/' . $filename . '.txt', $content);
    }
}

processDirectory($argv[1], $argv[2]);
