package com.wheelDestiny.HBase.MVTable.Tool;

import com.wheelDestiny.HBase.MVTable.mapper.ScanDataMapper;
import com.wheelDestiny.HBase.MVTable.reducer.InsertDataReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

public class HBaseMapperReduceTool implements Tool {
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance();

        job.setJarByClass(HBaseMapperReduceTool.class);

        TableMapReduceUtil.initTableMapperJob("wheeldestiny:emp",new Scan(), ScanDataMapper.class,ImmutableBytesWritable.class, Put.class,job);

        TableMapReduceUtil.initTableReducerJob("wheeldestiny:empbak", InsertDataReducer.class,job);


        boolean flag = job.waitForCompletion(true);

        //执行成功返回成功的状态码，执行失败返回失败的状态码
        return flag? JobStatus.State.SUCCEEDED.getValue():JobStatus.State.FAILED.getValue();
    }

    public void setConf(Configuration conf) {

    }

    public Configuration getConf() {
        return null;
    }
}
