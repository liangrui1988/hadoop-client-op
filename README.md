# hadoop-client-op
hadoop client op

hdfs ec bug
Cause data corruption, here do the recovery and verification related tools

```
1:check ec file & return sigle block error to datanode ip info
2:read ec file & skip block error datanode ip to copy new dir
3:orc check read (Verify according to your own file format)
4:if error block >1 (for all datanode read data)
5:for all datanode Still unable to recover,The data is completely blocked
```
