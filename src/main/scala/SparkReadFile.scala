package com.aengine.spark.app.ec

import com.aengine.spark.utils.ResourcesUtils.getEnv
import org.apache.commons.lang.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.util.control.Breaks.{break, breakable}

/**
 * 获取恢复文件的表列表 orc
 * hdfs dfs -ls -d hdfs://yycluster06/hive_warehouse_repl/-/-
 * hdfs dfs -ls -R  hdfs://yycluster06/hive_warehouse_repl/- | grep  -v 'drwxr'| awk '{print $8}' > repl_ec_file.txt
 * run hdfs://yycluster06/user/hdev/repl_ec_file.txt
 *
 *
 * -txt = hdfs://yycluster06/hive_warehouse/recover
 * spark.read.format("parquet").load("hdfs://yycluster06/hive_warehouse_repl/hiidosdk.db/yy_mbsdkdo_original/dt=20200217/part-00128-738ab778-505b-441f-ab1b-35ff430afa3d-c000").count()
 * spark.read.format("parquet").load("hdfs://yycluster06/hive_warehouse_repl/hiidosdk.db/yy_mbsdkdo_original/dt=20200217/part-00128-738ab778-505b-441f-ab1b-35ff430afa3d-c000").show()
 *
 */
object SparkReadFile {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val Array(allFileTab, filepath) = (args ++ Array(null, null)).slice(0, 2)

    val sparkBuilder = SparkSession.builder()
    if ("local".equals(getEnv("env"))) {
      sparkBuilder.master("local[*]")
    }
    val spark = sparkBuilder.appName("SparkReadFile").enableHiveSupport().getOrCreate()
    println("allFileTab==" + allFileTab)
    println("filepath==" + filepath)
    if (!StringUtils.isBlank(filepath)) {
      val fileDF = spark.read.format("parquet").load(filepath)
      fileDF.count()
      fileDF.show()
    } else if (!StringUtils.isBlank(allFileTab)) {
      val allFileTabDF = spark.read.text(allFileTab.trim)
      allFileTabDF.show()
      val allFileTabList = allFileTabDF.collectAsList();
      var seqFilePaths = Seq[String]()
      allFileTabList.forEach(r => {
        val filePath = r.getAs[String]("value")
        seqFilePaths :+= filePath
      })
      println("seqFilePaths size ==" + seqFilePaths.size)
      val parquetFiles = seqFilePaths.filter(x => {
        var isParquet = false //文件开头是不是parquet的前缀表
        breakable {
          for (pf <- parquetTab) {
            if (x.startsWith(pf)) isParquet = true; break //filter 掉  parquet开的文件
          }
        }
        isParquet
      })

      val orcSeq = seqFilePaths.filter(x => {
        var isParquet = false //文件开头是不是parquet的前缀表
        breakable {
          for (pf <- parquetTab) {
            if (x.startsWith(pf)) isParquet = true; break //filter 掉  parquet开的文件
          }
        }
        !isParquet
      })
      println("start orc ============")
      for (fPath <- orcSeq) {
        //println("orc is==" + fPath)
        try {
          val fileDF = spark.read.format("orc").load(fPath)
          fileDF.cache()
          println("show orc count" + fileDF.count())
          println("show orc show" + fileDF.show())
          //all data read
          val filterDF = fileDF.filter(r => r.get(1).toString.equals("1"))
          filterDF.show()
          println("fPath ok==" + fPath)
        } catch {
          case e: Exception => {
            println("fPath error==" + fPath)
            println("fPath getMessage: " + e.getMessage)
            e.getStackTrace
          }
        }
      }
      println("start parquet ============")
      for (fPath <- parquetFiles) {
        //println("parquet is==" + fPath)
        try {
          val fileDF = spark.read.format("parquet").load(fPath)
          fileDF.cache()
          println("show parquet count" + fileDF.count())
          println("show parquet show" + fileDF.show())
          //all data read
          val filterDF = fileDF.filter(r => r.get(1).toString.equals("1"))
          filterDF.show()
          println("fPath ok==" + fPath)
        } catch {
          case e: Exception => {
            println("fPath error==" + fPath)
            println("fPath getMessage: " + e.getMessage)
            e.getStackTrace
          }
        }
      }
    }
  }

  val parquetTab = Seq("hdfs://yycluster06/hive_warehouse_repl/hiidosdk.db/yy_mbsdkdo_original")


