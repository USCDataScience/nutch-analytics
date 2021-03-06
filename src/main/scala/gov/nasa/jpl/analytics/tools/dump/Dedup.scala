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
import scala.collection.JavaConverters._

/**
  * Created by karanjeetsingh on 8/31/16.
  */
class Dedup extends CliTool {

  import Dedup._

  @Option(name = "-m", aliases = Array("--master"))
  var sparkMaster: String = "local[*]"

  @Option(name = "-d", aliases = Array("--dumpDir"))
  var segmentDir: String = ""

  @Option(name = "-f", aliases = Array("--dumpFile"))
  var segmentFile: String = ""

  @Option(name = "-h", aliases = Array("--hash"))
  var hash: String = ""

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
    //CommonUtil.makeSafeDir(outputDir)

    // Generate a list of segment parts
    var parts: List[Path] = List()
    if (!segmentDir.isEmpty) {
      parts = SegmentReader.listDumpDir(segmentDir, config)
    } else if (!segmentFile.isEmpty) {
      parts = SegmentReader.listFromFile(segmentFile)
    } else {
      println("Please provide Segment Path")
      System.exit(1)
    }

    val hashRDD: RDD[Tuple2[String, String]] = sc.sequenceFile(hash.toString + File.separator + "part*")
    val hashes = hashRDD.collectAsMap()

    val jmap = new util.HashMap[String, String]()
    for ((k, v) <- hashes) {
      jmap.put(k, v)
    }

    // Converting all Segment parts to RDDs
    var rdds: Seq[RDD[Tuple2[String, String]]] = Seq()
    for (part <- parts) {
      dumpParam.part = part.toString
      val docRDD = sc.textFile(part.toString).map(doc => CommonUtil.toJson(doc.toString))
        .filter(doc => SegmentReader.filterDocs(dumpParam.part + "-" + doc.get(Constants.key.CDR_ID).toString, jmap))
        .map(doc => doc.toJSONString)
      docRDD.saveAsTextFile(outputDir + File.separator +
        dumpParam.part.substring(dumpParam.part.lastIndexOf(File.separator) + 1))
      //rdds :+= sc.sequenceFile[String, Content](part.toString)
    }

    sc.stop()
  }

}

object Dedup extends Loggable with Serializable {

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
    new Dedup().run(args)
  }

}