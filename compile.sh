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

sbt clean assemble-deps

sbt examples/assembly package
