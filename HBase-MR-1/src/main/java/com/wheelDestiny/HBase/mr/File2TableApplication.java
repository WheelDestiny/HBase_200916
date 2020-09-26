package com.wheelDestiny.HBase.mr;

import com.wheelDestiny.HBase.mr.Tool.File2TableTool;
import org.apache.hadoop.util.ToolRunner;

public class File2TableApplication {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new File2TableTool(),args);
    }
}
