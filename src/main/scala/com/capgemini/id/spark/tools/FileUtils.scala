package com.capgemini.id.spark.tools

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

object FileUtils {

  // We need to use HDFS FileSystem API to perform validations on input and output path

  /**
    * Will get the path from an HDFS directory, if the path does not exist the program will exit.
    * @param sc
    * @param inputPath Hdfs directory
    * @return a valid path
    */
  def getInputPath(sc: SparkContext, inputPath: String): Path = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(inputPath)
    val inputPathExists = fs.exists(path)
    if (!inputPathExists) {
      println("Invalid input path")
      throw new IllegalArgumentException("Input path does not exist")
    }
    return path
  }

  /**
    * Will delete an outputPath if it already exist
    * @param sc
    * @param outputPath Hdfs directory
    * @return
    */
  def deleteOutputPath(sc: SparkContext, outputPath: String) = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(outputPath)
    val outputPathExists = fs != null && fs.exists(path)
    if (outputPathExists) {
      println("Output path exist, proceeding to deletion")
      fs.delete(path, true)
    }
  }

  /**
    * Will overwrite an hdfs directory with the textfile
    * @param sc
    * @param rdd Data to be written to hdfs
    * @param outputPath Hdfs directory
    */
  def overwriteTextFile[T](sc: SparkContext, rdd : RDD[T], outputPath : String) = {
    deleteOutputPath(sc, outputPath)
    rdd.saveAsTextFile(outputPath)
  }

  /**
    * Saving the dataframe to a file
    * This is a curried version to simplify wr
    *
    * @param df Dataframe representing the object to be saved
    * @param path : Path destination
    * @param mode : SaveMode to use Append or Overwrite
    * @return
    */
  private def saveDf(partitionField: String)(formatType: String)(mode: SaveMode)(df: DataFrame, path: String) = {
    val dfw = df.write.mode(mode)
    if (! "".equals(partitionField)) {
      dfw.partitionBy(partitionField)
    } else {
      dfw
    }
    //todo log
    dfw.format(formatType).save(path)
  }


  /**
    * Append Dataframe to the parquet file
    * @param partitionField columns to partition the df
    * @return
    */
  def parquetAppend(partitionField: String, df: DataFrame, path : String) = {
    saveDf(partitionField)("parquet")(SaveMode.Append)(df, path)
  }

  /**
    * Overwrite the parquet file with the dataframe
    * @param partitionField columns to partition the df
    * @return
    */
  def parquetOverwrite(df: DataFrame, path: String, partitionField: String) = {
    saveDf(partitionField)("parquet")(SaveMode.Overwrite)(df, path)
  }

  /**
    * Do not partition the dataframe on hdfs. This will overwrite data on hdfs
    * @return
    */
  def saveNoPartitionOverwrite(df: DataFrame, path: String) = parquetOverwrite(df, path, "")
}
