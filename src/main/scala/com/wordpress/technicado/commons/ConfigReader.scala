package com.wordpress.technicado.commons

import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext


object ConfigReader {

  protected val properties = new PropertiesConfiguration()

  def readConfig(propertyFile: String, sparkContext: SparkContext): Unit = {
    val path = new Path(propertyFile)
    val fs = FileSystem.get(sparkContext.hadoopConfiguration)
    val inputStream = fs.open(path)
    properties.load(inputStream)
  }

  def readConfig(propertyFile: String): Unit =
    properties.load(propertyFile)

  def getString(propertyName: String): String =
    properties.getString(propertyName)

  def getLong(propertName: String): Long =
    properties.getLong(propertName)
}
