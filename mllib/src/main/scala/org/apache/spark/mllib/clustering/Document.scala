package org.apache.spark.mllib.clustering

import breeze.linalg.SparseVector


case class Document(docIdx: Int, content: SparseVector[Int])


// vim: set ts=4 sw=4 et:
