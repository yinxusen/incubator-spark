#!/bin/bash

NOW=`date +%Y%m%d-%H%M%S-%N`
#A=`date +%s`

VER="spark-bsp"
CUR="/home/yama/bdas/$VER"

ALS_INPUT=/home/yama/bdas/incubator-spark/mllib/data/als/test.data 
ALS_INPUT=/home/yama/bdas/data/smallnetflix_mm.train_csv
ALS_OUTPUT=/tmp/als/

PR_INPUT="hdfs://linux-c0002:8020/spark/web-Google.txt"
PR_INPUT=/home/yama/bdas/data/web-small
PR_INPUT=/home/yama/bdas/data/soc-LiveJournal1.txt
PR_INPUT=/home/yama/bdas/data/soc-pokec-relationships.txt
PR_INPUT=/home/yama/bdas/data/web-Google.txt 

LR_INPUT=hdfs://queen:9000/xusen

LR_INPUT_DISK=/home/yama/data/xusen

MLLIB=org.apache.spark.mllib

MASTER=local
MASTER=spark://172.16.126.183:7077 

EXAMPLE_ALS="./run-example org.apache.spark.examples.SparkALS $MASTER 10"
MLLIB_ALS="./spark-class $MLLIB.recommendation.ALS $MASTER $ALS_INPUT 5 3 $ALS_OUTPUT"
PR="$CUR/bin/run-example org.apache.spark.examples.SparkPageRank $MASTER $PR_INPUT 3 60"

ITER=300
PART=120
RUN="bin/spark-class"

LR_NEW="$RUN $MLLIB.classification.LogisticRegressionWithSGD $MASTER $LR_INPUT 1.0 45 15"
LR_OLD="$RUN $MLLIB.classification.LogisticRegressionWithSGDAlt $MASTER $LR_INPUT 1.0 40 15"

LR_NEW_DISK="$RUN $MLLIB.classification.LogisticRegressionWithSGD $MASTER $LR_INPUT_DISK 1.0 $ITER $PART"
LR_OLD_DISK="$RUN $MLLIB.classification.LogisticRegressionWithSGDAlt $MASTER $LR_INPUT_DISK 1.0 $ITER $PART"

SVM_NEW="$RUN $MLLIB.classification.SVMWithSGD $MASTER $LR_INPUT_DISK 1.0 $ITER $PART"
SVM_OLD="$RUN $MLLIB.classification.SVMWithSGDAlt $MASTER $LR_INPUT_DISK 1.0 $ITER $PART"

#-------- do the real run --------

DST=/tmp/spark-yama

	START=`date +%s`
	$SVM_NEW 2>&1 | tee $DST/log.tmp
	FINISH=`date +%s`
	LOSS=`cat $DST/log.tmp | grep "GradientDescent finished" | awk '{print $NF}'`
	mv $DST/log.tmp $DST/$START-$[$FINISH - $START]-${LOSS:0:5}-i$ITER-p$PART-SVM_NEW.log

exit 0

for((ii=0; ii<5; ii++)); do
	START=`date +%s`
	$LR_NEW_DISK 2>&1 | tee $DST/log.tmp
	FINISH=`date +%s`
	LOSS=`cat $DST/log.tmp | grep "GradientDescent finished" | awk '{print $NF}'`
	mv $DST/log.tmp $DST/$START-$[$FINISH - $START]-${LOSS:0:5}-i$ITER-p$PART-LR_NEW_DISK.log
	sleep 5
	START=`date +%s`
	$LR_OLD_DISK 2>&1 | tee $DST/log.tmp
	FINISH=`date +%s`
	LOSS=`cat $DST/log.tmp | grep "GradientDescent finished" | awk '{print $NF}'`
	mv $DST/log.tmp $DST/$START-$[$FINISH - $START]-${LOSS:0:5}-i$ITER-p$PART-LR_OLD_DISK.log
done
