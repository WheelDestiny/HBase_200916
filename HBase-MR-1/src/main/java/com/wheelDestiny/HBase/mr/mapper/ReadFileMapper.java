package com.wheelDestiny.HBase.mr.mapper;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReadFileMapper extends Mapper<LongWritable,Text, ImmutableBytesWritable, Put> {
    private StringBuilder rowkey = new StringBuilder();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        rowkey.delete(0,rowkey.length());
        String[] values = value.toString().split(",");

        rowkey.append(values[0]);

        byte[] bs = Bytes.toBytes(rowkey.toString());
        Put put = new Put(bs);

        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(values[1]));

        context.write(new ImmutableBytesWritable(bs),put);
    }
}
