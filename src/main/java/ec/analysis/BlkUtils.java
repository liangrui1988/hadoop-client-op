package ec.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.server.namenode.NamenodeFsck;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.token.Token;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BlkUtils {

    public static void main(String[] args) {
        Map<String, Object> fsckResult = new HashMap<>();
        String newFormat = "";
        try {
            //fsckResult = doWork(context.getUser(), config, blkid, connectionFactory);
            String finalBlkid = "blkid";
//                    fsckResult = UserGroupInformation.getCurrentUser().doAs(
//                            new PrivilegedExceptionAction<Map<String, Object>>() {
//                                @Override
//                                public Map<String, Object> run() throws Exception {
//                                    Configuration conf=   context.getConfiguration();
//                                    conf.set("dfs.http.policy","HTTP_ONLY");
//                                    Credentials ccc = context.getCredentials();
//                                    for (Token t : ccc.getAllTokens()) {
//                                        log.info("doAs doAs for token:" + t.toString());
//                                    }
//                                    URLConnectionFactory connectionFactory = URLConnectionFactory.newDefaultURLConnectionFactory(conf);
//                                    return doWork(context.getUser(), conf, finalBlkid, connectionFactory);
//                                }
//                            });


        } catch (Exception e) {
            System.err.println("error dowork");
            e.printStackTrace();
        }

        if (fsckResult.containsKey("filePath")) {
            String filePath = (String) fsckResult.get("filePath");
            newFormat += "," + filePath;
        }
        if (fsckResult.containsKey("datanodes")) {
            List<String> datanodes = (List<String>) fsckResult.get("datanodes");
            String dnStr = String.join("__", datanodes);
            newFormat += "," + dnStr;
        }
    }

    //Connecting to namenode via http://fs-hiido-yycluster06-yynn1.hiido.host.yydevops.com:50070/fsck?ugi=hdfs&blockId=blk_-9223372036296597648+&path=%2F
    public static synchronized Map<String, Object>  doWork(Configuration conf, String blockId, URLConnectionFactory connectionFactory) throws IOException, AuthenticationException {
        Map<String, Object> result = new HashMap<String, Object>();
        result.put("errCode", -1);
        final StringBuilder url = new StringBuilder();
        url.append("/fsck?ugi=hdev");
        StringBuilder sb = new StringBuilder();
        //sb.append(blockId + " ");
        sb.append(blockId + " ");
        url.append("&blockId=").append(URLEncoder.encode(sb.toString(), "UTF-8"));
        URI namenodeAddress = null;
        try {
            namenodeAddress = getCurrentNamenodeAddress(conf);
        } catch (IOException ioe) {
            System.err.println("FileSystem is inaccessible due to:\n"
                    + ioe.toString());
            ioe.printStackTrace();
        }
        if (namenodeAddress == null) {
            //Error message already output in {@link #getCurrentNamenodeAddress()}
            System.err.println("DFSck exiting.");
            return result;
        }
        url.insert(0, namenodeAddress.toString());
        url.append("&path=").append(URLEncoder.encode(
                Path.getPathWithoutSchemeAndAuthority(new Path("/")).toString(), "UTF-8"));
        //System.err.println("Connecting to namenode via " + url.toString());
        URL path = new URL(url.toString());
        URLConnection connection;
        try {
            connection = connectionFactory.openConnection(path, true);
        } catch (AuthenticationException e) {
            e.printStackTrace();
            throw new IOException(e);
        }
        InputStream stream = connection.getInputStream();
        BufferedReader input = new BufferedReader(new InputStreamReader(
                stream, "UTF-8"));
        String line;
        List<String> datanodes = new ArrayList<String>();
        String filePath = "";
        try {
            while ((line = input.readLine()) != null) {
                //out.println(line);
                if (line.startsWith("Block belongs to:")) {
                    filePath = line.replace("Block belongs to:", "").trim();
                    result.put("filePath", filePath);
                }
                if (line.startsWith("Block replica on datanode/rack")) {
                    String dnHostname = line.replace("Block replica on datanode/rack:", "").split("com/")[0].trim() + "com";
                    String hostSimple=dnHostname.replace(".hiido.host.yydevops.com", "").
                            replace(".hiido.host.int.yy.com", "").
                            replace("fs-hiido-dn-", "");
                    datanodes.add(hostSimple);
                }
            }
        } finally {
            input.close();
        }
        result.put("datanodes", datanodes);
        return result;
    }


    private static URI getCurrentNamenodeAddress(Configuration conf) throws IOException {
        conf.set("mapreduce.client.genericoptionsparser.used", "true");
        conf.set("fs.defaultFS", "hdfs://yycluster06");
        DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);
        return DFSUtil.getInfoServer(HAUtil.getAddressOfActive(fs), conf,
                DFSUtil.getHttpClientScheme(conf));
    }
}
