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

import gov.nasa.jpl.analytics.base.{Loggable, CliTool}
import gov.nasa.jpl.analytics.model.CdrDumpParam
import gov.nasa.jpl.analytics.nutch.SegmentReader
import gov.nasa.jpl.analytics.util.{CommonUtil, Constants}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.nutch.protocol.Content
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json.simple.JSONObject
import org.kohsuke.args4j.Option

import scala.collection.JavaConversions._

/**
  * Created by karanjeetsingh on 8/31/16.
  */
class Cdrv2Dump extends CliTool {

  import Cdrv2Dump._

  @Option(name = "-m", aliases = Array("--master"))
  var sparkMaster: String = "local[*]"

  @Option(name = "-s", aliases = Array("--segmentDir"))
  var segmentDir: String = ""

  @Option(name = "-f", aliases = Array("--segmentFile"))
  var segmentFile: String = ""

  @Option(name = "-l", aliases = Array("--linkDb"))
  var linkDb: String = ""

  @Option(name = "-t", aliases = Array("--type"))
  var docType: String = ""

  @Option(name = "-o", aliases = Array("--outputDir"))
  var outputDir: String = ""


  var sc: SparkContext = _

  def init(): Unit = {
    val conf = new SparkConf()
    conf.setAppName("CDRv2Dumper")
      .setMaster(sparkMaster)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.classesToRegister", "org.apache.nutch.protocol.Content")
      .set("spark.kryoserializer.buffer.max", "2040m")
    sc = new SparkContext(conf)
  }

  override def run(): Unit = {

    // Initialize SparkContext
    init()
    val config: Configuration = sc.hadoopConfiguration
    val dumpParam: CdrDumpParam = new CdrDumpParam()
    dumpParam.docType = docType

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
      parts = SegmentReader.listFromDir(segmentDir, config)
    } else if (!segmentFile.isEmpty) {
      parts = SegmentReader.listFromFile(segmentFile)
    } else {
      println("Please provide Segment Path")
      System.exit(1)
    }

    // Converting all Segment parts to RDDs
    var rdds: Seq[RDD[Tuple2[String, Content]]] = Seq()
    for (part <- parts) {
      rdds :+= sc.sequenceFile[String, Content](part.toString)
    }
    println("Number of Segments to process: " + rdds.length)

    // Union of all RDDs
    val segRDD:RDD[Tuple2[String, Content]] = sc.union(rdds)

    // Filtering & Operations
    //TODO: If content type is image, get inLinks
    val filteredRDD =segRDD.filter({case(text, content) => SegmentReader.filterUrl(content)})
    val cdrRDD = filteredRDD.map({case(text, content) => SegmentReader.toCdrV2(text, content, dumpParam)})

    // Deduplication & Dumping Segments
    val dumpRDD = cdrRDD.map(doc => (doc.get(Constants.key.CDR_ID).toString, doc))
                        .reduceByKey((key1, key2) => key1)
                        .map({case(id, doc) => new JSONObject(doc).toJSONString})

    dumpRDD.saveAsTextFile(outputDir)

    // Write to Local File System
    /*
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputDir + File.separator + "out.json")))
    for (doc <- docs) {
      writer.write(doc + "\n")
    }
    writer.close()
    */

    sc.stop()
  }

}

object Cdrv2Dump extends Loggable with Serializable {

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
    new Cdrv2Dump().run(args)
  }

}
