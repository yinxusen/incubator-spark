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


import java.io.PrintWriter
import java.io.DataOutputStream
import java.nio.file.Files
import java.nio.file.{Path => JPath}
import java.nio.file.{Paths => JPaths}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils._

class SmallTextFilesSuite extends FunSuite with BeforeAndAfterAll {
  private var sc: SparkContext = _
  private var dfs: MiniDFSCluster = _
  private val conf: Configuration = new Configuration()

  override def beforeAll() {
    sc = new SparkContext("local", "test")
    conf.set("dfs.datanode.data.dir.perm", "775")
    dfs = new MiniDFSCluster(conf, 4, true,
                             Array("/rack0", "/rack0", "/rack1", "/rack1"),
                             Array("host0", "host1", "host2", "host3"))
  }

  override def afterAll() {
    if (dfs != null) dfs.shutdown()
    sc.stop()
    System.clearProperty("spark.driver.port")
  }


  private def createHDFSFile(fs: FileSystem, inputDir: Path, fileName: String, size: Int) = {
    val out: DataOutputStream = fs.create(new Path(inputDir, fileName), true, 4096, 2, 512, null);
    for (i <- 0 to size) {
      out.writeChars(s"Hello - $i\n")
    }
    out.close();
    System.out.println("Wrote HDFS file")
  }


  /**
   * This code will test the behaviors on HDFS.
   * There are two aspects to test:
   * First is all files are read.
   * Second is the fileNames are read correctly.
   */
  test("Small file input || HDFS IO") {
    val fs: FileSystem = dfs.getFileSystem
    val dir = "/foo/"
    val inputDir: Path = new Path(dir)
    val fileNames = Array("part-00000", "part-00001", "part-00002")
    val fileSizes = Array(1000, 100, 10)

    fileNames.zip(fileSizes).foreach {
      case (fname, size) =>
        createHDFSFile(fs, inputDir, fname, size)
    }

    println(s"name node is ${dfs.getNameNode.getNameNodeAddress.getHostName}")
    println(s"name node port is ${dfs.getNameNodePort}")

    val hdfsAddressDir =
      s"hdfs://${dfs.getNameNode.getNameNodeAddress.getHostName}:${dfs.getNameNodePort}${dir}"
    println(s"HDFS address dir is ${hdfsAddressDir}")

    val res = smallTextFiles(sc, hdfsAddressDir, 2).collect()

    assert(res.size == fileNames.size)

    val fileNameSet = res.map(_._1).toSet

    for (fname <- fileNames) {
      assert(fileNameSet.contains(fname))
    }
  }

  private def createNativeFile(inputDir: JPath, fileName: String, size: Int) = {
    val out = new PrintWriter(s"${inputDir.toString}/$fileName")
    for (i <- 0 to size) {
      out.println(s"Hello - $i")
    }
    out.close()
    println("Wrote native file")
  }

  /**
   * This code will test the behaviors on native file system.
   * There are two aspects to test:
   * First is all files are read.
   * Second is the fileNames are read correctly.
   */
  test("Small file input || native disk IO") {
    val dir = Files.createTempDirectory("smallfiles")
    println(s"native disk address is ${dir.toString}")
    val fileNames = Array("part-00000", "part-00001", "part-00002")
    val fileSizes = Array(1000, 100, 10)

    fileNames.zip(fileSizes).foreach {
      case (fname, size) =>
        createNativeFile(dir, fname, size)
    }

    val res = smallTextFiles(sc, dir.toString, 2).collect()

    assert(res.size == fileNames.size)

    val fileNameSet = res.map(_._1).toSet

    for (fname <- fileNames) {
      assert(fileNameSet.contains(fname))
    }

    fileNames.foreach { fname =>
      Files.deleteIfExists(JPaths.get(s"${dir.toString}/$fname"))
    }
    Files.deleteIfExists(dir)
  }

}
