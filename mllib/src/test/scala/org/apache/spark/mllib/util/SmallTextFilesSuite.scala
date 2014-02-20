/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.util


import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import org.apache.spark.SparkContext

import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import java.io.DataOutputStream

import org.apache.hadoop.conf.Configuration


class SmallTextFilesSuite extends FunSuite with BeforeAndAfterAll {
  private var sc: SparkContext = _
  private var dfs: MiniDFSCluster = _
  private val conf: Configuration = new Configuration()

  override def beforeAll() {
    sc = new SparkContext("local", "test")
    dfs = new MiniDFSCluster(conf, 4, true,
                             Array("/rack0", "/rack0", "/rack1", "/rack1"),
                             Array("host0", "host1", "host2", "host3"))
  }

  override def afterAll() {
    if (dfs != null) dfs.shutdown()
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  private def createInputs(fs: FileSystem, inputDir: Path, fileName: String) = {
    val out: DataOutputStream = fs.create(new Path(inputDir, fileName), true, 4096, 2, 512, null);
    for (i <- 0 to 1000) {
      out.writeChars(s"Hello - $i\n")
    }
    out.close();
    System.out.println("Wrote file")
  }


  test("Small file input || HDFS IO") {
    val fs: FileSystem = dfs.getFileSystem
    val dir = "/foo/"
    val inputDir: Path = new Path(dir)
    val fileName = "part-00000"
    createInputs(fs, inputDir, fileName)
    println(s"name node is ${dfs.getNameNode.getNameNodeAddress.getHostName}")
    println(s"name node port is ${dfs.getNameNodePort}")
    val hdfsAddressDir = s"hdfs://${dfs.getNameNode.getNameNodeAddress.getHostName}:${dfs.getNameNodePort}${dir}"
    println(s"HDFS address dir is ${hdfsAddressDir}")
    val res2 = sc.textFile(hdfsAddressDir, 2).collect()
    for (s <- res2) {
      println(s)
    }
    val res = sc.smallTextFiles(hdfsAddressDir, 2).collect()
    println(res.size)
    for (s <- res) {
      println(s._1)
      println(s._2)
    }
    assert(res.head == fileName)
  }
}
