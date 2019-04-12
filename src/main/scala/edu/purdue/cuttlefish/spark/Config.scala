package edu.purdue.cuttlefish.spark

import org.apache.spark.sql.SparkSession

object Config {
    val CUTTLEFISH_HOME = sys.env("CUTTLEFISH_HOME")
    val MASTER = ""
    val SPARK_PORT = "7077"
    val HDFS_PORT = "9001"

    object FileSystem extends Enumeration {
        val LOCAL, HDFS = Value
    }

    val fileSystem = FileSystem.LOCAL

    def getDefaultSpark(appName: String = "Unnamed App", master: String = "local") = {

        val masterURL =
            if (master == "standalone" || master == "spark")
                "spark://" + Config.MASTER + ":" + Config.SPARK_PORT
            else
                master

        val spark = SparkSession
          .builder()
          .appName(appName)
          .master(masterURL)
          .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        spark
    }

    def getHDFSPath(path: String) = "hdfs://" + Config.MASTER + ":" + Config.HDFS_PORT + path

    def getLocalPath(path: String) = "file://" + path

}
