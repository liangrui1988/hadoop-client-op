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
import org.apache.log4j.Logger;

/**
 * hadoop jar hdfs-client-op-1.0-SNAPSHOT.jar com.yy.bigdata.orc.OpenFileLine /home/liangrui/ec_error.log 
 */
public class OpenFileLine {

    private static Logger logger = Logger.getLogger(OpenFileLine.class);

    public static void main(String[] args) {
        String filePath = "";
        System.out.println("main args " + args);
        if (args.length >= 1) {
            filePath = args[0];
        }
        logger.info("start. "+filePath);

        List<String> readLine=OrcUtils.readLine(filePath);
        if(readLine ==null || readLine.size()<=0){
            logger.info("read file is null " + filePath);
            System.exit(0);
        }

        try {
        EcUtils check =new EcUtils();
        Configuration conf = HdfsCUtils.getCfg();
        DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(conf);
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("hdev@YYDEVOPS.COM", "/home/liangrui/hdev.keytab");

        for (String line:readLine) {
          Map<String,String> result=check.checkEC(line,dfs);
          if(result==null||result.size()<=0){
              System.out.println("==" + line);
              continue;
          }
            logger.info("start line === " + line);
            String status=result.get("status");
            if("0".equals(status)){
                logger.info("file ec status is ok=== " + line);
                continue;
            }
            if("1".equals(status)){
                logger.info("file ec status is -1 === " + line);
                continue;
            }

            if("-1".equals(status)){
                String msg=result.get("msg");
                String skipIp=msg.split("ip is")[1].split("block is")[0].trim();
                if(StringUtils.isBlank(skipIp)){
                    logger.info(msg +"=ec 异常 无法解析出ip=" + line);
                    continue;
                }
                logger.info("start  Compensatory recovery");
                conf.set("ext.skip.ip", skipIp);
                logger.info("skip ip is "+skipIp);
                dfs = (DistributedFileSystem) FileSystem.get(conf);
                // UserGroupInformation.setConfiguration(conf);
                //UserGroupInformation.loginUserFromKeytab("hdev@YYDEVOPS.COM", "/home/liangrui/hdev.keytab");
                String copy_dest = line.replace("hive_warehouse", "hive_warehouse_repl");
                FileUtil.copy(dfs,new Path(line), dfs,new Path(copy_dest),false,conf);
                //目标文件是否是ORC
                String mkdir=line.substring(0,line.lastIndexOf("/"));
                dfs.mkdirs(new Path(mkdir));
                boolean is_normal = OrcUtils.readOrcCheck(copy_dest,skipIp);
                if (is_normal) {
                    logger.info("recovery orc file is normal ==="+line);
                } else {
                    logger.error("recovery orc file is failure ==="+line);
                    //删除异常恢复的orc
                    dfs.delete(new Path(copy_dest),false);
                }
            }

        }
        }catch (Exception e){
            e.printStackTrace();
        }

    }

}
