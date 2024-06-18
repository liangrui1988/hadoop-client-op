package ec;

import com.yy.bigdata.utils.HdfsCUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.client.impl.BlockReaderRemote;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.tools.AdminHelper;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Logger;

public  class  EcUtils {

    private DFSClient client;
    private int dataBlkNum;
    private int parityBlkNum;
    private int cellSize;
    private boolean useDNHostname;
    private CachingStrategy cachingStrategy;
    private int stripedReadBufferSize=65536;
    private CompletionService<Integer> readService;
    private RawErasureEncoder encoder;
    private BlockReader[] blockReaders;

    /**
     * status [1=无法检查错误，-1 EC异常，0=正常文件]
     * @param file
     * @param dfs
     * @return
     * @throws IOException
     */
    public    Map<String,String>   checkEC(String file,DistributedFileSystem dfs) throws IOException {
        Map<String,String> map=new HashMap<String,String>();
        map.put("status","1");
        Path path = new Path(file);
        org.apache.hadoop.conf.Configuration cfg=HdfsCUtils.getCfg();
        this.client = dfs.getClient();

        FileStatus fileStatus;
        try {
            fileStatus = dfs.getFileStatus(path);
        } catch (FileNotFoundException e) {
            map.put("msg","File " + file + " does not exist.");
            return map;
        }

        if (!fileStatus.isFile()) {
            map.put("msg","File " + file + " is not a regular file.");
            return map;
        }
        if (!dfs.isFileClosed(path)) {
            map.put("msg","File " + file + " is not closed.");
            return map;
        }
        this.useDNHostname = cfg.getBoolean(DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME,
                DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT);
        this.cachingStrategy = CachingStrategy.newDefaultStrategy();
        this.stripedReadBufferSize = cfg.getInt(
                DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_BUFFER_SIZE_KEY,
                DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_BUFFER_SIZE_DEFAULT);

        LocatedBlocks locatedBlocks = client.getLocatedBlocks(file, 0, fileStatus.getLen());
        if (locatedBlocks.getErasureCodingPolicy() == null) {
            map.put("msg","File " + file + " is not erasure coded.");
            return map;
        }
        ErasureCodingPolicy ecPolicy = locatedBlocks.getErasureCodingPolicy();
        this.dataBlkNum = ecPolicy.getNumDataUnits();
        this.parityBlkNum = ecPolicy.getNumParityUnits();
        this.cellSize = ecPolicy.getCellSize();
        this.encoder = CodecUtil.createRawEncoder(cfg, ecPolicy.getCodecName(),
                new ErasureCoderOptions(
                        ecPolicy.getNumDataUnits(), ecPolicy.getNumParityUnits()));
        int blockNum = dataBlkNum + parityBlkNum;

        ThreadPoolExecutor cThreadPool=DFSUtilClient.getThreadPoolExecutor(blockNum, blockNum, 60,
                new LinkedBlockingQueue<>(), "read-", false);
        this.readService = new ExecutorCompletionService<>(cThreadPool);
        this.blockReaders = new BlockReader[dataBlkNum + parityBlkNum];
         List<String> ips=new ArrayList<>();
        for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
            //System.out.println("Checking EC block group: blk_" + locatedBlock.getBlock().getBlockId());
            LocatedStripedBlock blockGroup = (LocatedStripedBlock) locatedBlock;

            try {
                verifyBlockGroup(blockGroup);
                //System.out.println("Status: OK");
                map.put("status","0");
                map.put("msg","\nAll EC block group status: OK");
            } catch (Exception e) {
                map.put("status","-1");
                map.put("msg","Status: ERROR, message: " + e.getMessage());
            } finally {
                closeBlockReaders();
            }
        }
        cThreadPool.shutdown();
        //System.out.println("\nAll EC block group status: OK");
        return map;
    }

    private void closeBlockReaders() {
        for (int i = 0; i < blockReaders.length; i++) {
            if (blockReaders[i] != null) {
                IOUtils.closeStream(blockReaders[i]);
                blockReaders[i] = null;
            }
        }
    }


    private void verifyBlockGroup(LocatedStripedBlock blockGroup) throws Exception {
        final LocatedBlock[] indexedBlocks = StripedBlockUtil.parseStripedBlockGroup(blockGroup,
                cellSize, dataBlkNum, parityBlkNum);

        int blockNumExpected = Math.min(dataBlkNum,
                (int) ((blockGroup.getBlockSize() - 1) / cellSize + 1)) + parityBlkNum;
        if (blockGroup.getBlockIndices().length < blockNumExpected) {
            throw new Exception("Block group is under-erasure-coded.");
        }

        long maxBlockLen = 0L;
        DataChecksum checksum = null;
        for (int i = 0; i < dataBlkNum + parityBlkNum; i++) {
            LocatedBlock block = indexedBlocks[i];
            if (block == null) {
                blockReaders[i] = null;
                continue;
            }
            if (block.getBlockSize() > maxBlockLen) {
                maxBlockLen = block.getBlockSize();
            }
            BlockReader blockReader = createBlockReader(block.getBlock(),
                    block.getLocations()[0], block.getBlockToken());
            if (checksum == null) {
                checksum = blockReader.getDataChecksum();
            } else {
                assert checksum.equals(blockReader.getDataChecksum());
            }
            blockReaders[i] = blockReader;
        }
        assert checksum != null;
        int bytesPerChecksum = checksum.getBytesPerChecksum();
        int bufferSize = stripedReadBufferSize < bytesPerChecksum ? bytesPerChecksum :
                stripedReadBufferSize - stripedReadBufferSize % bytesPerChecksum;
        final ByteBuffer[] buffers = new ByteBuffer[dataBlkNum + parityBlkNum];
        final ByteBuffer[] outputs = new ByteBuffer[parityBlkNum];
        for (int i = 0; i < dataBlkNum + parityBlkNum; i++) {
            buffers[i] = ByteBuffer.allocate(bufferSize);
        }
        for (int i = 0; i < parityBlkNum; i++) {
            outputs[i] = ByteBuffer.allocate(bufferSize);
        }
        long positionInBlock = 0L;
        while (positionInBlock < maxBlockLen) {
            final int toVerifyLen = (int) Math.min(bufferSize, maxBlockLen - positionInBlock);
            List<Future<Integer>> futures = new ArrayList<>(dataBlkNum + parityBlkNum);
            for (int i = 0; i < dataBlkNum + parityBlkNum; i++) {
                final int fi = i;
                futures.add(this.readService.submit(() -> {
                    BlockReader blockReader = blockReaders[fi];
                    ByteBuffer buffer = buffers[fi];
                    buffer.clear();
                    buffer.limit(toVerifyLen);
                    int readLen = 0;
                    if (blockReader != null) {
                        int toRead = buffer.remaining();
                        while (readLen < toRead) {
                            int nread = blockReader.read(buffer);
                            if (nread <= 0) {
                                break;
                            }
                            readLen += nread;
                        }
                    }
                    while (buffer.hasRemaining()) {
                        buffer.put((byte) 0);
                    }
                    buffer.flip();
                    return readLen;
                }));
            }
            for (int i = 0; i < dataBlkNum + parityBlkNum; i++) {
                futures.get(i).get(1, TimeUnit.MINUTES);
            }
            ByteBuffer[] inputs = new ByteBuffer[dataBlkNum];
            System.arraycopy(buffers, 0, inputs, 0, dataBlkNum);
            for (int i = 0; i < parityBlkNum; i++) {
                outputs[i].clear();
                outputs[i].limit(toVerifyLen);
            }
            this.encoder.encode(inputs, outputs);
            for (int i = 0; i < parityBlkNum; i++) {
                //outputs is  recalculate
                // buffers is src data
                if (!buffers[dataBlkNum + i].equals(outputs[i])) {
                    // add ip and blk info
                    LocatedBlock lBlock =indexedBlocks[dataBlkNum + i];
                    throw new Exception("EC compute result not match.:"+"ip is "+lBlock.getLocations()[0].getIpAddr()+
                            " block is : "+lBlock.getB().getBlockId());
                }
            }
            positionInBlock += toVerifyLen;
        }
    }

    private BlockReader createBlockReader(ExtendedBlock block, DatanodeInfo dnInfo,
                                          Token<BlockTokenIdentifier> token) throws IOException {
        InetSocketAddress dnAddress = NetUtils.createSocketAddr(dnInfo.getXferAddr(useDNHostname));
        Peer peer = client.newConnectedPeer(dnAddress, token, dnInfo);
        return BlockReaderRemote.newBlockReader(
                "dummy", block, token, 0,
                block.getNumBytes(), true, "", peer, dnInfo,
                null, cachingStrategy, -1);
    }
    protected void finalize() throws Throwable {
        super.finalize();
    }
}
