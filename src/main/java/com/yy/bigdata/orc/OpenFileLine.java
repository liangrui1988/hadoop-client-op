package com.yy.bigdata.orc;

import com.yy.bigdata.utils.HdfsCUtils;
import ec.EcUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.tracing.SpanReceiverInfo;
import org.apache.log4j.Logger;

/**
 * create time 20240611
 * create by rui liang
 * <p>
 * nohup hadoop jar hdfs-client-op-1.0-SNAPSHOT.jar com.yy.bigdata.orc.OpenFileLine /home/liangrui/tmp/line_file/line_warehouse_db_ts_webyyhb_clean_day3  orc >  /home/liangrui/tmp/run_logc/ts_webyyhb_clean_day3.log &
 * nohup hadoop jar hdfs-client-op-1.0-SNAPSHOT.jar com.yy.bigdata.orc.OpenFileLine /home/liangrui/tmp/line_file/line_freshman_db_audit_log  text >  /home/liangrui/tmp/run_logc/freshman_db_audit_log.log &
 */
public class OpenFileLine {

    private static Logger logger = Logger.getLogger(OpenFileLine.class);

    /**
     * 1:check ec file & return sigle block error to datanode ip info
     * 2:read ec file & skip block error datanode ip to copy new dir
     * 3:orc check read (Verify according to your own file format)
     * 4:if error block >1 (for all datanode read data)
     * 5:for all datanode Still unable to recover,The data is completely blocked
     *
     * @param args file list path
     */
    public static void main(String[] args) {
        String filePath = "";
        String fileFormat = "orc";
        System.out.println("main args " + args);
        if (args.length >= 1) {
            filePath = args[0];
        }
        if (args.length >= 2) {
            fileFormat = args[1];
        }
        logger.info("start. " + filePath);

        List<String> readLine = OrcUtils.readLine(filePath);
        if (readLine == null || readLine.size() <= 0) {
            logger.info("read file is null " + filePath);
            System.exit(0);
        }

        try {
            EcUtils check = new EcUtils();
            Configuration conf = HdfsCUtils.getCfg();
            DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(conf);
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("hdev@YYDEVOPS.COM", "/home/liangrui/hdev.keytab");

            for (String line : readLine) {
                if (StringUtils.isBlank(line)) {
                    continue;
                }
                Map<String, String> result = check.checkEC(line, dfs);
                if (result == null || result.size() <= 0) {
                    System.out.println("==" + line);
                    continue;
                }
                logger.info("start line === " + line);
                String status = result.get("status");
                if ("0".equals(status)) {
                    logger.info("file ec status is ok=== " + line);
                    continue;
                }
                if ("1".equals(status)) {
                    logger.info("file ec status is -1 === " + line);
                    continue;
                }

                if ("-1".equals(status)) {
                    String msg = result.get("msg");
                    String skipIp = null;
                    try {
                        skipIp = msg.split("ip is")[1].split("block is")[0].trim();
                    } catch (Exception e) {
                        logger.info(msg + "=ec error not get ip=" + line);
                        e.printStackTrace();
                    }
                    if (StringUtils.isBlank(skipIp)) {
                        logger.info("ec error not get ip ===" + line);
                        continue;
                    }
                    logger.info("start  Compensatory recovery");
                    conf.set("ext.skip.ip", skipIp);
                    logger.info("skip ip is " + skipIp);
                    dfs = (DistributedFileSystem) FileSystem.get(conf);
                    String copy_dest = "";
                    if ("orc".equals(fileFormat)) {
                        copy_dest = line.replace("hive_warehouse", "hive_warehouse_repl");
                    } else if ("text".equals(fileFormat)) {
                        copy_dest = line.replace("hive_warehouse", "hive_warehouse/recover");
                    }

                    FileUtil.copy(dfs, new Path(line), dfs, new Path(copy_dest), false, conf);
                    //targer file is ORC
                    String mkdir = line.substring(0, line.lastIndexOf("/"));
                    dfs.mkdirs(new Path(mkdir));
                    boolean is_normal = true;
                    if ("orc".equals(fileFormat)) {
                        is_normal = OrcUtils.readOrcCheck(copy_dest, "");
                    } else if ("text".equals(fileFormat)) {
                        Map<String, String> dest_result = check.checkEC(copy_dest, dfs);//If this method is accurate
                        if (!"0".equals(dest_result.get("status"))) is_normal = false;
                    }
                    if (is_normal) {
                        logger.info("recovery  file is normal ===" + line);
                    } else {
                        // for exclude datanode
                        List<String> ipList = HdfsCUtils.getIps(dfs.getClient(), line);
                        System.out.println(ipList);
                        String[] ips = ipList.toArray(new String[0]);
                        boolean is_for_recovery = false;
                        for (int i = 0; i <= ips.length - 2; i++) {
                            if (is_for_recovery) {
                                continue;
                            }
                            for (int j = i + 1; j <= ips.length - 1; j++) {
                                String skip_ips = ips[i] + "," + ips[j];
                                conf.set("ext.skip.ip", skip_ips);
                                logger.info("skip ip is " + skip_ips);
                                FileUtil.copy(dfs, new Path(line), dfs, new Path(copy_dest), false, conf);
                                if ("orc".equals(fileFormat)) {
                                    is_normal = OrcUtils.readOrcCheck(copy_dest, "");
                                }
                                if ("text".equals(fileFormat)) {
                                    Map<String, String> dest_result = check.checkEC(copy_dest, dfs);//If this method is accurate
                                    if ("0".equals(dest_result.get("status"))) is_normal = true;
                                }

                                if (is_normal) {
                                    logger.info("for recovery orc file is normal ===" + line);
                                    is_for_recovery = true;
                                    break;
                                }
                            }
                        }
                        if (!is_for_recovery) {
                            logger.error("recovery orc file is failure ===" + line);
                            //delete error orc
                            dfs.delete(new Path(copy_dest), false);
                        }

                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
