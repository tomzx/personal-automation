<?php

$directory = 'E:\Tom\Documents\EXCEL\Books';
$sourceFile = '_BookReadingTemplate.xlsx';
$newFilename = $argv[1].'.xlsx';

$sourcePath = $directory.'/'.$sourceFile;
$targetPath = $directory.'/'.$newFilename;

if (file_exists($targetPath)) {
	return;
}

copy($sourcePath, $targetPath);
shell_exec('call "'.$targetPath.'"');
