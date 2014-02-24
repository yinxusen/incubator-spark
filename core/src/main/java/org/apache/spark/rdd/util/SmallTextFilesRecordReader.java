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

package org.apache.spark.rdd.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.util.LineReader;

public class SmallTextFilesRecordReader implements RecordReader<BlockwiseTextWritable, Text> {
    private long startOffset;
    private long end;
    private long pos;
    private int totalLength;
    private Path path;

    private LineReader reader;

    public SmallTextFilesRecordReader(
            CombineFileSplit split,
            Configuration conf,
            Reporter reporter,
            Integer index)
            throws IOException {
        path = split.getPath(index);
        startOffset = split.getOffset(index);
        pos = startOffset;
        totalLength = (int) split.getLength(index);
        end = startOffset + totalLength;

        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream fileIn = fs.open(path);
        fileIn.seek(startOffset);
        reader = new LineReader(fileIn);
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    @Override
    public float getProgress() throws IOException {
        if (startOffset == end) return 0;
        return Math.min(1.0f, (pos - startOffset) / (float) (end - startOffset));
    }

    public BlockwiseTextWritable createKey() {
        return new BlockwiseTextWritable();
    }

    public Text createValue() {
        return new Text();
    }

    /**
     * We will read an entire block here.
     * Note that if there are some files, which are large than the block size of HDFS,
     * are cut by HDFS, then there will be some fragments.
     * We should preserve all file names and offset in each block,
     * to recovery an entire file.
     */
    @Override
    public boolean next(BlockwiseTextWritable key, Text value) throws IOException {
        key.fileName = path.getName();
        key.offset = pos;
        value.clear();

        if (pos >= end) {
            return false;
        }

        StringBuilder totalContent = new StringBuilder();
        Text buffer = new Text();

        while (pos < end) {
            pos += reader.readLine(buffer);
            totalContent.append(buffer.toString());
            totalContent.append('\n');
        }

        // TODO describe edge case here
        if (totalLength < totalContent.length()) {
            totalContent.delete(totalLength, totalContent.length());
        }

        value.set(totalContent.toString());

        return true;
    }
}
