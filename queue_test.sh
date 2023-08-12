#!/bin/sh

for i in $(seq 1 10); do
  redis-cli RPUSH QUEUE:TESTCHAN_1 "{\"function\":\"tests.fixtures.test\",\"params\":\"sleep=int:5,lock=LOCK_1,msg=Hello from TESTCHAN_1! $i\"}"
done

for i in $(seq 1 10); do
  redis-cli RPUSH QUEUE:TESTCHAN_2 "{\"function\":\"tests.fixtures.test\",\"params\":\"sleep=int:5,lock=LOCK_2,msg=Hello from TESTCHAN_2! $i\"}"
done

for i in $(seq 1 10); do
  redis-cli RPUSH QUEUE:FAST "{\"function\":\"tests.fixtures.test\",\"params\":\"sleep=int:1,lock=LOCK_3,msg=Hello from FAST! $i\"}"
done
