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
LR_INPUT=/home/yama/data/webspam_wc_normalized_trigram.svm
LR_INPUT=/home/yama/data/webspam_wc_normalized_unigram.svm

MLLIB=org.apache.spark.mllib

MASTER=local
MASTER=spark://172.16.126.183:7077 

ITER=20
PART=120
RUN="bin/spark-class"

EXAMPLE_ALS="./run-example org.apache.spark.examples.SparkALS $MASTER 10"
MLLIB_ALS="./spark-class $MLLIB.recommendation.ALS $MASTER $ALS_INPUT 5 3 $ALS_OUTPUT"
PR="$CUR/bin/run-example org.apache.spark.examples.SparkPageRank $MASTER $PR_INPUT 3 60"

PARA5="$MASTER $LR_INPUT 1.0 $ITER $PART"
PARA6="$MASTER $LR_INPUT 1.0 0.1 $ITER $PART"

LR_NEW="$RUN $MLLIB.classification.LogisticRegressionWithSGD $PARA5"
LR_OLD="$RUN $MLLIB.classification.LogisticRegressionWithSGDAlt $PARA5"

SVM_NEW="$RUN $MLLIB.classification.SVMWithSGD $PARA6"
SVM_OLD="$RUN $MLLIB.classification.SVMWithSGDAlt $PARA6"

LASSO_NEW="$RUN $MLLIB.regression.LassoWithSGD $PARA6"
LASSO_OLD="$RUN $MLLIB.regression.LassoWithSGDAlt $PARA6"

LINEAR_NEW="$RUN $MLLIB.regression.LinearRegressionWithSGD $PARA5"
LINEAR_OLD="$RUN $MLLIB.regression.LinearRegressionWithSGDAlt $PARA5"

RIDGE_NEW="$RUN $MLLIB.regression.RidgeRegressionWithSGD $PARA6"
RIDGE_OLD="$RUN $MLLIB.regression.RidgeRegressionWithSGDAlt $PARA6"

#-------- do the real run --------

DST=/tmp/spark-yama

#for((ii=0; ii<5; ii++)); do
	START=`date +%s`
	$LINEAR_NEW 2>&1 | tee $DST/log.tmp
	FINISH=`date +%s`
	LOSS=`cat $DST/log.tmp | grep "GradientDescent finished" | awk '{print $NF}'`
	mv $DST/log.tmp $DST/$START-$[$FINISH - $START]-${LOSS:0:5}-i$ITER-p$PART-LINEAR_NEW.log
	#sleep 5
	#START=`date +%s`
	#$SVM_OLD 2>&1 | tee $DST/log.tmp
	#FINISH=`date +%s`
	#LOSS=`cat $DST/log.tmp | grep "GradientDescent finished" | awk '{print $NF}'`
	#mv $DST/log.tmp $DST/$START-$[$FINISH - $START]-${LOSS:0:5}-i$ITER-p$PART-SVM_OLD.log
#done
