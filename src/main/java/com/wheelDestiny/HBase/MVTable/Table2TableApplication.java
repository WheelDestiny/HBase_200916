package com.wheelDestiny.HBase.MVTable;

import com.wheelDestiny.HBase.MVTable.Tool.HBaseMapperReduceTool;
import org.apache.hadoop.util.ToolRunner;

public class Table2TableApplication {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new HBaseMapperReduceTool(),args);
    }
}
