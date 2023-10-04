#!/bin/bash

# 清空或创建 test.txt 文件
> test.txt

# 循环运行 go test 一百次，并将输出追加到 test.txt 文件
for i in {1..100}; do
  echo "Running test $i"
  go test -run 2A >> test.txt
  sleep 10
done

echo "Tests completed. Output saved to test.txt."
