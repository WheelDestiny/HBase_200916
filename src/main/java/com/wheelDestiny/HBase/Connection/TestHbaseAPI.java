package com.wheelDestiny.HBase.Connection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 测试Hbase的API
 */
public class TestHbaseAPI {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        System.out.println(connection+"!!!!");
        Admin admin = connection.getAdmin();

        TableName tableName =TableName.valueOf("wheeldestiny:emp");
        boolean exists = admin.tableExists(tableName);
        System.out.println(exists);


        Table table = connection.getTable(tableName);

        Scan scan = new Scan();

        ResultScanner scanner = table.getScanner(scan);
        scanner.forEach((res)->{
            for (Cell cell : res.rawCells()) {
                System.out.println("value = "+Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("rowkey = "+Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("family = "+Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("column = "+Bytes.toString(CellUtil.cloneQualifier(cell)));
            }
        });

    }
}
