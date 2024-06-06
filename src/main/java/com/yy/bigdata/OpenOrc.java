package com.yy.bigdata;

import com.yy.bigdata.utils.HdfsCUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.Arrays;


public class OpenOrc {
    static String filePath = "/ec_hive_warehouse/haokan3.db/haokan_dm_aid_ver_sys_entry_cube_info_day/dt=2023-12-10/part-00000-a8d3b9d7-67a8-4acb-ac94-87e78c837470-c000";

    public static void main(String[] args) {
        try {
            Configuration conf = HdfsCUtils.getCfg();
            conf.set("ext.skip.ip","10.12.65.2,10.12.65.239");
            DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);
            UserGroupInformation.loginUserFromKeytab("test-hiido2@TESTCLUSTER.COM", "/home/liangrui/test-hiido2.keytab");
            UserGroupInformation.setConfiguration(conf);
            DFSClient dfs = fs.getClient();
            dfs.getClientName();
            //取出路径上的localBlkopenToDn
            LocatedBlocks locatedBlocks = dfs.getLocatedBlocks
                    (filePath, 0, 1024);
 /*           String[] skipIp=new String[]{"10.12.65.2","10.12.65.239"};
            System.out.println("main skip ip "+ Arrays.toString(skipIp));
            DFSInputStream dis= dfs.openToDn(filePath,dfs.getConf().getIoBufferSize(),true,skipIp);*/
            fs.copyToLocalFile(false,new Path(filePath),new Path("/home/liangrui/part-00000-a8d3b9d7-67a8-4acb-ac94-87e78c837470-c000"),true);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

/*    public DFSInputStream open(DFSClient dfs,HdfsPathHandle fd, int buffersize,
                               boolean verifyChecksum) throws IOException {
        dfs.checkOpen();
        String src = fd.getPath();
        try (TraceScope ignored = dfs.newPathTraceScope("newDFSInputStream", src)) {
            HdfsLocatedFileStatus s = dfs.getLocatedFileInfo(src, true);
            fd.verify(s); // check invariants in path handle
            LocatedBlocks locatedBlocks = s.getLocatedBlocks();
            return dfs.openInternal(locatedBlocks, src, verifyChecksum);
        }
    }*/

}
