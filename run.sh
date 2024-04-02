#!/bin/bash

s=''

for i in $(cat .env)
do
  export $i
  echo setting $i
done

$@