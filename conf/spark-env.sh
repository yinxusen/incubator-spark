#!/usr/bin/env bash
export SPARK_MASTER_IP=172.16.126.183
export SPARK_MASTER=yama@172.16.126.183:/home/yama/bdas/spark-bsp
# for hurrah
export SPARK_WORKER_MEMORY=10g
export SPARK_MEM=8g
# for queen
#export SPARK_WORKER_MEMORY=72g
#export SPARK_MEM=64g
export SPARK_JAVA_OPTS+=" -Dspark.gradientDescent.innerIteration=30"
export SPARK_JAVA_OPTS+=" -Dspark.default.dimension=2001"
