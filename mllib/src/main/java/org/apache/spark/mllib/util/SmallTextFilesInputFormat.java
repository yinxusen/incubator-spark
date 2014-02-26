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

package org.apache.spark.mllib.util;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

/**
 * The specific InputFormat reads small text files in HDFS or local disk. It will be called by
 * HadoopRDD to generate new RecordReader.
 */
public class SmallTextFilesInputFormat
        extends CombineFileInputFormat<BlockwiseTextWritable, Text> {

    @Override
    public RecordReader<BlockwiseTextWritable, Text> getRecordReader(
            InputSplit split,
            JobConf conf,
            Reporter reporter) throws IOException {
        return new CombineFileRecordReader<BlockwiseTextWritable, Text>(
                conf,
                (CombineFileSplit)split,
                reporter,
                (Class)SmallTextFilesRecordReader.class);
    }

    /**
     * Splitable should be set to false in the context of reading small files, to ensure that all
     * blocks of a single file keeps in the same split. In this way, HadoopRDD will keep one file
     * in a partition, which is shuffle-avoid when combining blocks of a file together into an
     * entire file.
     */
    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }
}
