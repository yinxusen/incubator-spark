#!/bin/bash

for i in `ls`; do sed -i 1"s/^/${i}, /" ${i}; done
for i in `ls`; do awk '{printf("%s", $0)}' ${i} > ${i}txt; done
for i in `ls`; do tr -d "\015\n" < ${i} > ${i}unix; done
