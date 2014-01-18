#!/bin/bash

./sbin/stop-all.sh
echo "-------- stop spark --------"
sleep 2
./sbin/start-all.sh
echo "------- restrt spark --------"
