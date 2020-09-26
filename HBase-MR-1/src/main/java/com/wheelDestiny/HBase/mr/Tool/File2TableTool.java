package com.wheelDestiny.HBase.mr.Tool;

import com.wheelDestiny.HBase.mr.mapper.ReadFileMapper;
import com.wheelDestiny.HBase.mr.reducer.InsertDataReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;

public class File2TableTool implements Tool {
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance();

        job.setJarByClass(File2TableTool.class);

        //format
        FileInputFormat.addInputPath(job,new Path("hdfs://nn1.hadoop:9000/user/wheeldestiny26/data/student.csv"));

        //hdfs ==> hbase
        //mapper
        job.setMapperClass(ReadFileMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //reducer
        TableMapReduceUtil.initTableReducerJob("wheeldestiny:empbak", InsertDataReducer.class,job);

        return job.waitForCompletion(true)? JobStatus.State.SUCCEEDED.getValue() :JobStatus.State.FAILED.getValue();
    }

    public void setConf(Configuration conf) {

    }

    public Configuration getConf() {
        return null;
    }
}
