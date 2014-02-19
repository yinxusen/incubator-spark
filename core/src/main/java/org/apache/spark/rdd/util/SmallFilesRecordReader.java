package org.apache.spark.rdd.util;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.util.LineReader;

import org.apache.hadoop.conf.Configuration;

public class SmallFilesRecordReader implements RecordReader<FileLineWritable, Text> {
    private long startOffset;
    private long end;
    private long pos;
    private FileSystem fs;
    private Path path;

    private FSDataInputStream fileIn;
    private LineReader reader;

    public SmallFilesRecordReader (CombineFileSplit split, Configuration conf, Reporter reporter, Integer index)
            throws IOException{
        this.path = split.getPath(index);
        fs = this.path.getFileSystem(conf);
        this.startOffset = split.getOffset(index);
        this.end = startOffset + split.getLength(index);
        fileIn = fs.open(path);
        reader = new LineReader(fileIn);
        this.pos = startOffset;
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

    public FileLineWritable createKey() {
        FileLineWritable retKey = new FileLineWritable();
        retKey.fileName = path.getName();
        return retKey;
    }

    public Text createValue() {
        Text retValue = new Text();
        return retValue;
    }

    @Override
    public boolean next(FileLineWritable key, Text value) throws IOException {
        key.offset = pos;
        int newSize = 0;
        if (pos < end) {
            newSize = reader.readLine(value);
            pos += newSize;
        }
        if (newSize == 0) {
            return false;
        } else{
            return true;
        }
    }
}
