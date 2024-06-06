package com.yy.bigdata;

import com.yy.bigdata.utils.HdfsCUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;


public class TestClientToDn {
    static String filePath = "/ec_hive_warehouse/haokan3.db/haokan_dm_aid_ver_sys_entry_cube_info_day/dt=2023-12-10/part-00000-a8d3b9d7-67a8-4acb-ac94-87e78c837470-c000";
    private static DatanodeProtocolClientSideTranslatorPB namenode;
    final SaslDataTransferClient saslClient;

    private final static InetSocketAddress NN_ADDR = new InetSocketAddress(
            "localhost", 5020);


    public TestClientToDn(SaslDataTransferClient saslClient, Configuration conf) throws IOException {

        AtomicBoolean nnFallbackToSimpleAuth = new AtomicBoolean(false);

        this.saslClient = new SaslDataTransferClient(
                conf, DataTransferSaslUtil.getSaslPropertiesResolver(conf),
                TrustedChannelResolver.getInstance(conf), nnFallbackToSimpleAuth);
    }


    public static void main(String[] args) {
        try {
            Configuration conf = HdfsCUtils.getCfg();
            DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);
            UserGroupInformation.loginUserFromKeytab("test-hiido2@TESTCLUSTER.COM", "/home/liangrui/test-hiido2.keytab");
            UserGroupInformation.setConfiguration(conf);
            DFSClient dfs = fs.getClient();
            dfs.getStoragePolicies();
            //LocatedBlock
            BlockLocation[] blockLocation = dfs.getBlockLocations
                    (filePath, 0, 1024);
            System.out.println(Arrays.toString(blockLocation));

            LocatedBlocks locatedBlocks = dfs.getLocatedBlocks
                    (filePath, 0, 1024);
            System.out.println(locatedBlocks);

            for (int i = 0; i < blockLocation.length; i++) {
                BlockLocation location = blockLocation[i];
                System.out.println(blockLocation[i]);
                System.out.println(Arrays.toString(blockLocation[i].getHosts()));
                for (String hostx : blockLocation[i].getHosts()
                ) {
                    System.out.println("host is===" + hostx);
                }
            }
            // DFSUtilClient.connectToDN(dn,  300,null);
            StorageLocation location = StorageLocation.parse(filePath);

            System.out.println("location is===" + location);


           //DataNode.instantiateDataNode();


        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
