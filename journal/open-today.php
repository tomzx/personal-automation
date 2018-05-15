<?php

$filename = 'by-date/' . date('Y/m') . '/' . date('d') . '.md';
shell_exec('code '.$filename);
