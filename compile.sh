#!/bin/bash

:<<note
gen-idea
clean 
assemble-deps 
examples/assembly
compile
package 
assembly
note

/home/yama/bdas/spark-bsp/sbt/sbt assemble-deps package
