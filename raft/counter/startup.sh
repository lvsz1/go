#!/usr/bin/env bash

./counter --id 1 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379  --addr :8600 > node1.log &
./counter --id 2 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379  --addr :8601 > node2.log &
./counter --id 3 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379  --addr :8602 > node3.log &
