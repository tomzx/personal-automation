<?php

$filename = 'buffer/' . date('Y/m') . '/' . date('d') . '.md';
shell_exec('code '.$filename);
