package org.apache.spark.rdd.util;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

public class SmallTextFilesInputFormat extends
        CombineFileInputFormat<FileLineWritable, Text> {

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public RecordReader<FileLineWritable, Text> getRecordReader(
            InputSplit split,
            JobConf conf,
            Reporter reporter) throws IOException {
        return new CombineFileRecordReader(
                conf,
                (CombineFileSplit) split,
                reporter,
                (Class) SmallTextFilesRecordReader.class);
    }

    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

}