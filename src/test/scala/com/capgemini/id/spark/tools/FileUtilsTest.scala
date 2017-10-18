package com.capgemini.id.spark.tools


import java.io.{File, PrintWriter}

import com.github.sakserv.minicluster.impl.HdfsLocalCluster
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

class FileUtilsTest extends FlatSpec with BeforeAndAfterAll {

  private val master = "local[2]"
  private val appName = "example-spark"

  var sc: SparkContext = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)
  }

  "A path" must "exist" in {
    val src : String = "src/test/resources/mytest.csv"
    val p = FileUtils.getInputPath(sc, src)
    assert(p != null && "mytest.csv" == p.getName)
  }

  it should "produce IllegalArgumentException when file does not exist" in {
    assertThrows[IllegalArgumentException] {
      FileUtils.getInputPath(sc, "")
    }
  }

  "an rdd" should "be able to overwrite" in {
    val file : String = "src/test/resources/myoverwrite.csv"

    // Lets verify that the deleteOutputPath is going to delete a file by throwing an assertion
    val p = FileUtils.getInputPath(sc, file)
    FileUtils.deleteOutputPath(sc, file)

    assertThrows[IllegalArgumentException] {
      val deletedPath = FileUtils.getInputPath(sc, file)
    }

    // ok good then lets write a new directory
    val rdd = sc.parallelize(Seq("Hello","world"));
    FileUtils.overwriteTextFile(sc, rdd, file)
    val newPath = FileUtils.getInputPath(sc, file)
    assert(newPath != null && "myoverwrite.csv" == newPath.getName)
  }



}
