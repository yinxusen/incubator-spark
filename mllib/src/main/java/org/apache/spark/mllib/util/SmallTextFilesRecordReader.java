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

    private static final byte[] LFs = {'\n'};

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
     *
     * Note that '\n' will stand for all other line breaks, such as "\r\n".
     */
    @Override
    public boolean next(BlockwiseTextWritable key, Text value) throws IOException {
        key.fileName = path.getName();
        key.offset = pos;
        value.clear();

        if (pos >= end) {
            return false;
        }

        Text buffer = new Text();
        Text oneTimeBuffer = new Text();

        while (pos < end) {
            pos += reader.readLine(oneTimeBuffer);
            buffer.append(oneTimeBuffer.getBytes(), 0, oneTimeBuffer.getLength());
            buffer.append(LFs, 0, LFs.length);
        }

        // If the totalContent.length() larger than the totalLength, it indicates that
        // the readLine() function read from the next block for a complete line.
        // But it is wrong if we do not delete the extra characters. Because the next
        // block reader will also read these characters.
        //
        // For instance (It could not be a practical example, but just for clarification):
        //
        // "abcdefg
        //  higklmn
        //  opqrst
        //  uvwxyz"
        //
        // If we set the block size to 5 bytes, a line will be splited, it will become
        //
        // "abcde
        //  fg[\n]hi
        //  jklmn
        //  opqrs
        //  t[\n]uvwx
        //  yz"
        //
        // In this case, the first block will read "abcdefg" out, while the second block
        // will read "fg[\n]higklmn".

        if (totalLength < buffer.getLength()) {
            value.set(buffer.getBytes(), 0, totalLength);
        } else {
            value.set(buffer.getBytes());
        }

        return true;
    }
}
