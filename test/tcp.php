<?php

function compare($a, $b) {
  if ($a !== $b) {
    throw new Exception("Assert failed : <".dump($a)."> != <".dump($b).">");
  }
}

function read($sock, $min = 0) {
  $s = "";
  while(substr($s, -2) !== "\r\n" || strlen($s) < $min) {
    $s .= fread($sock, 2048);
  }
  return $s;
}

function generateRandomString($length = 10) {
  $characters = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
  $charactersLength = strlen($characters);
  $randomString = '';
  for ($i = 0; $i < $length; $i++) {
      $randomString .= $characters[rand(0, $charactersLength - 1)];
  }
  return $randomString;
}

$sock = fsockopen("localhost", 6379);
fwrite($sock, "*2\r\n$3\r\nDEL\r\n$1\r\na\r\n");
compare(read($sock), ":0\r\n");
fwrite($sock, "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$4\r\nabcd\r\n*2\r\n$3\r\nGET\r\n$1\r\na\r\n");
compare(read($sock), "+OK\r\n");
compare(read($sock, 6), "$4\r\nabcd\r\n");
for($x = 10; $x < 1500; $x ++) {
  $k = generateRandomString($x);
  fwrite($sock, "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$".$x."\r\n".$k."\r\n*2\r\n$3\r\nGET\r\n$1\r\na\r\n");
  compare(read($sock), "+OK\r\n");
  compare(read($sock, $x), "$".$x."\r\n".$k."\r\n");
}
fwrite($sock, "*2\r\n$3\r\nDEL\r\n$1\r\na\r\n");
compare(read($sock), ":1\r\n");
fwrite($sock, "QUIT\r\n");
fclose($sock);
