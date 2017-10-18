package com.capgemini.id.spark.helpers

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * ApplicationID is a trait to describe application launch, reports and error management
  * This should be a trait but for interoperability with Java abstract class was chosen.
  */
abstract class ApplicationID {
  /** Application name to pass to the Spark Context */
  val applicationName = ""

  /** Use by default a SQLContext, as it is much easier to test, some aggregation are only usable in HiveContext **/
  val needHiveContext = false

  /**
    * Base directory to buid your directory tree such as dev, test, integ, prod.
    * Usually represent the phase that you are working onNames are free
    */
  var phase = None: Option[String]

  /** SparkContext */
  var sc = None: Option[SparkContext]

  /** SQLContext */
  var sqlContext = None: Option[SQLContext]

  /** Main program
    *
    * @param args Only one arg use, the phase
    */
  def main(args: Array[String]): Unit = {
    var exitCode = 0

    if (args.length < 1) {
      throw ArgumentsException("Using script : [phase]")
    }

    phase = Some(args(0))

    //TODO log
    sqlContext = Some(getConf(applicationName, needHiveContext))
    sc = Some(sqlContext.get.sparkContext)

    try {

      run(phase.get, sc.get, sqlContext.get)

    } catch {
      case t: Throwable => {
        //TODO log
        t.printStackTrace()
        exitCode = 1
      }
    } finally {
      //TODO log
    }
    System.exit(exitCode)
  }

  /** Each application will override run to implement its pipeline i */
  def run(phase: String, sc: SparkContext, sqlContext: SQLContext): Unit

  /** Getting the corresponding configuration for Spark */
  def getConf(appTitle: String, hiveContext: Boolean = false) = {

    val appName = "[%s] - %s".format(appTitle, phase.get)
    val conf = new SparkConf().setAppName(appName)

    val sc = new SparkContext(conf)
    val sqlContext = {
      if (hiveContext) {
        new HiveContext(sc)
      } else {
        new SQLContext(sc)
      }
    }
    //TODO log
    sqlContext
  }
}
