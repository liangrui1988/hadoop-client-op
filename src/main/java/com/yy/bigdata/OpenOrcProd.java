package com.yy.bigdata;

import com.yy.bigdata.utils.HdfsCUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.security.UserGroupInformation;

import java.net.InetAddress;
import java.util.Arrays;


public class OpenOrcProd {
    // String filePath = "/hive_warehouse/hiidodw.db/yy_mbsdkquality_original/dt=20210828/hm=2259/1622184811191-0-22147.zlib";

    public static void main(String[] args) {
        String filePath = "";
        System.out.println("main args " + args);
        if (args.length >= 1) {
            filePath = args[0];
        }
        if(StringUtils.isBlank(filePath)){
            System.out.println("filePath is blank");
            System.exit(0);
        }

        try {
            System.out.println("filePath is=="+filePath);
            Configuration conf = HdfsCUtils.getCfg();
            DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("hdev@YYDEVOPS.COM", "/home/liangrui/hdev.keytab");
            DFSClient dfs = fs.getClient();
            dfs.getClientName();
            //取出路径上的localBlkopenToDn
            BlockLocation[] blockLocation = dfs.getBlockLocations
                    (filePath, 0, 1024);
            for (int i = 0; i < blockLocation.length; i++) {
                BlockLocation location = blockLocation[i];
                System.out.println(blockLocation[i]);
                System.out.println(Arrays.toString(blockLocation[i].getHosts()));
                for (String hostx : blockLocation[i].getHosts()) {
                    System.out.println("host is===" + hostx);
                    InetAddress address = InetAddress.getByName(hostx);
                    String ip = address.getHostAddress();
                    System.out.println("for ext.skip.ip is===" + ip);
                    UserGroupInformation.setConfiguration(conf);
                    conf.set("ext.skip.ip", ip);
                    DistributedFileSystem fs2 = (DistributedFileSystem) FileSystem.get(conf);
                    fs2.copyToLocalFile(false, new Path(filePath),
                            new Path("/home/liangrui/skip_ip/" + ip + "_skip_file"), true);
                }
            }
            //locatedBlocks.getLocatedBlocks().
 /*           String[] skipIp=new String[]{"10.12.65.2","10.12.65.239"};
            System.out.println("main skip ip "+ Arrays.toString(skipIp));
            DFSInputStream dis= dfs.openToDn(filePath,dfs.getConf().getIoBufferSize(),true,skipIp);*/
            //fs.copyToLocalFile(false,new Path(filePath),new Path("/home/liangrui/skip_ip/1622184811191-0-22147.zlib"),true);
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
