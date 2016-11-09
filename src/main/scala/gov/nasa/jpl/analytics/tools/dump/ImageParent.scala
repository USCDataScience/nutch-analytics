/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gov.nasa.jpl.analytics.tools.dump

import java.io._
import java.util

import gov.nasa.jpl.analytics.base.{Loggable, CliTool}
import gov.nasa.jpl.analytics.model.CdrDumpParam
import gov.nasa.jpl.analytics.nutch.SegmentReader
import gov.nasa.jpl.analytics.util.{CommonUtil, Constants}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.nutch.crawl.{Inlink, LinkDb, Inlinks}
import org.apache.nutch.metadata.Metadata
import org.apache.nutch.protocol.Content
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json.simple.JSONObject
import org.kohsuke.args4j.Option

import scala.collection.JavaConversions._

/**
  * Created by karanjeetsingh on 8/31/16.
  */
class ImageParent extends CliTool {

  import ImageParent._

  @Option(name = "-m", aliases = Array("--master"))
  var sparkMaster: String = "local[*]"

  @Option(name = "-s", aliases = Array("--segmentDir"))
  var segmentDir: String = ""

  @Option(name = "-d", aliases = Array("--dumpDir"))
  var dumpDir: String = ""

  @Option(name = "-o", aliases = Array("--outputDir"))
  var outputDir: String = ""


  var sc: SparkContext = _

  def init(): Unit = {
    val conf = new SparkConf()
    conf.setAppName("UniqueUrls")
      .setMaster(sparkMaster)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.classesToRegister", "java.util.HashSet,java.util.HashMap")
      .set("spark.kryoserializer.buffer.max", "2040m")
    conf.registerKryoClasses(Array(classOf[Content], classOf[Inlinks], classOf[Inlink], classOf[Metadata]))
    sc = new SparkContext(conf)
  }

  override def run(): Unit = {

    // Initialize SparkContext
    init()
    val config: Configuration = sc.hadoopConfiguration
    val dumpParam: CdrDumpParam = new CdrDumpParam()

    // Check & Create Output Directory
    val fs: FileSystem = FileSystem.get(config)
    val outPath: Path = new Path(outputDir)
    if (fs.exists(outPath)) {
      println("Please provide a non existing directory path")
      System.exit(1)
    }

    // Generate a list of segment parts
    var parts: List[Path] = List()
    if (!segmentDir.isEmpty) {
      parts = SegmentReader.listFromDir(segmentDir, config)
    } else {
      println("Please provide Segment Path")
      System.exit(1)
    }

    // Converting all Segment parts to RDDs of [URL, Timestamp]
    var rdds: Seq[RDD[Tuple2[String, String]]] = Seq()
    for (part <- parts) {
      rdds :+= sc.sequenceFile[String, Content](part.toString)
                .map(doc => (doc._1, CommonUtil.formatTimestamp(doc._2.getMetadata.get("Date"))))
    }

    // Generate Url Timestamp Map
    val urlTsRdd = sc.union(rdds)
    val urlTs = urlTsRdd.collectAsMap
    val urlTsMap = new util.HashMap[String, String]()
    for ((k, v) <- urlTs) {
      urlTsMap.put(k, v)
    }


    // Generate a list of dump parts
    parts = List()
    if (!dumpDir.isEmpty) {
      parts = SegmentReader.listDumpDir(dumpDir, config)
    } else {
      println("Please provide Dump Path")
      System.exit(1)
    }

    // Converting all Dump parts to RDDs
    var dumpRdds: Seq[RDD[String]] = Seq()
    for (part <- parts) {
      val tempRDD = sc.textFile(part.toString)

      //tempRDD
        //.map{doc => CommonUtil.toJson(doc.toString)}
        //.take(2).foreach(println)

      val dumpRDD = tempRDD
        .map{doc => CommonUtil.toJson(doc.toString)}
        .filter(doc => SegmentReader.filterImages(doc))
        .map(doc => SegmentReader.addParentUrl(doc, urlTsMap))
        .map(doc => (new JSONObject(doc)).toJSONString)

      dumpRDD.saveAsTextFile(outputDir + File.separator
        + part.toString.substring(part.toString.lastIndexOf(File.separator) + 1))

    }

    // Union of all RDDs & Joining it with LinkDb
    //val dumpRDD:RDD[String] = sc.union(dumpRdds)

    // Filtering & Operations
    //TODO: If content type is image, get inLinks


    sc.stop()
  }

}

object ImageParent extends Loggable with Serializable {

  /**
    * Used for reading/writing to database, files, etc.
    * Code From the book "Beginning Scala"
    * http://www.amazon.com/Beginning-Scala-David-Pollak/dp/1430219890
    */
  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
    try { f(param) } finally { param.close() }

  def appendJson(filename: String, data: String) =
    using(new FileWriter(filename, true)) {
      fileWriter => using(new PrintWriter(fileWriter)) {
        printWriter => printWriter.println(data)
      }
    }

  def printJson(map: Map[String, Any]): Unit = {
    val obj:JSONObject = new JSONObject(map)
    println(obj.toJSONString)
  }

  def main(args: Array[String]) {
    new ImageParent().run(args)
  }

}
