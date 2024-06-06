package com.yy.bigdata;

import com.yy.bigdata.utils.HdfsCUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.net.InetAddress;
import java.util.Arrays;


public class OpenOrcProdTest {
    // String filePath = "/hive_warehouse/hiidodw.db/yy_mbsdkquality_original/dt=20210828/hm=2259/1622184811191-0-22147.zlib";

    public static void main(String[] args) {
        String filePath = "";
        String skipIp = "";
        System.out.println("main args " + args);
        if (args.length >= 1) {
            filePath = args[0];
        }
        if (args.length >= 2) {
            skipIp = args[1];
        }
        if (StringUtils.isBlank(filePath)) {
            System.out.println("filePath is blank");
            System.exit(0);
        }

        try {
            System.out.println("filePath is==" + filePath);
            Configuration conf = HdfsCUtils.getCfg();
            DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("hdev@YYDEVOPS.COM", "/home/liangrui/hdev.keytab");
            DFSClient dfs = fs.getClient();
            dfs.getClientName();
            //取出路径上的localBlkopenToDn
            BlockLocation[] blockLocation = dfs.getBlockLocations
                    (filePath, 0, 1024);
            System.out.println("for ext.skip.ip is===" + skipIp);
            UserGroupInformation.setConfiguration(conf);
            conf.set("ext.skip.ip", skipIp);
            DistributedFileSystem fs2 = (DistributedFileSystem) FileSystem.get(conf);
            fs2.copyToLocalFile(false, new Path(filePath),
                    new Path("/home/liangrui/skip_ip/" + skipIp + "_skip_file"), true);

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
