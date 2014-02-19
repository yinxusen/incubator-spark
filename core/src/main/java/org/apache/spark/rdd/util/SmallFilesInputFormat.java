package org.apache.spark.rdd.util;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

public class SmallFilesInputFormat extends
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
                (Class) SmallFilesRecordReader.class);
    }

}