<?php

$directory = 'buffer/' . date('Y/m');
if (!file_exists($directory)) {
	mkdir($directory, 0777, true);
}

$filename = $directory . '/' . date('d') . '.md';
touch($filename);