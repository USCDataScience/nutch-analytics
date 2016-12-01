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
import gov.nasa.jpl.analytics.util.{Constants}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.nutch.crawl._
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
class CrawlDumper extends CliTool {

  import CrawlDumper._

  @Option(name = "-m", aliases = Array("--master"))
  var sparkMaster: String = "local[*]"

  @Option(name = "-sd", aliases = Array("--segmentDir"))
  var segmentDir: String = ""

  @Option(name = "-sf", aliases = Array("--segmentFile"))
  var segmentFile: String = ""

  @Option(name = "-ldb", aliases = Array("--linkDb"))
  var linkDb: String = ""

  @Option(name = "-cdb", aliases = Array("--crawlDb"))
  var crawlDb: String = ""


  var sc: SparkContext = _

  def init(): Unit = {
    val conf = new SparkConf()
    conf.setAppName("CDRv2Dumper")
      .setMaster(sparkMaster)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.classesToRegister", "java.util.HashSet,java.util.HashMap")
      .set("spark.kryoserializer.buffer.max", "2040m")
      .set("spark.driver.maxResultSize", "2048m")
    conf.registerKryoClasses(Array(classOf[Content], classOf[Inlinks],
      classOf[Inlink], classOf[Metadata], classOf[CrawlDb]))
    sc = new SparkContext(conf)
  }


  def getFileTree(f: File): Stream[File] =
    f #:: (if (f.isDirectory) f.listFiles().toStream.flatMap(getFileTree)
    else Stream.empty)


  override def run(): Unit = {

    // Initialize Spark Context
    init()
    val config: Configuration = sc.hadoopConfiguration

    // Reading LinkDb
    var linkDbParts: List[Path] = List()
    linkDbParts = SegmentReader.listFromDir(linkDb, config, LinkDb.CURRENT_NAME)
    var linkDbRdds: Seq[RDD[Tuple2[String, Inlinks]]] = Seq()
    for (part <- linkDbParts) {
      linkDbRdds :+= sc.sequenceFile[String, Inlinks](part.toString)
    }
    println("Number of LinkDb Segments to process: " + linkDbRdds.length)
    val linkDbRdd = sc.union(linkDbRdds)


    // If Read CrawlDb Locally
    //var crawlDbParts: Stream[File] = Stream()
    //val partPattern: String = ".*" + File.separator + "current" +
    //  File.separator + "part-[0-9]{5}" + File.separator + "data"
    //crawlDbParts = getFileTree(new File(crawlDb)).filter(_.getAbsolutePath.matches(partPattern))

    // Reading CrawlDb
    var crawlDbParts: List[Path] = List()
    crawlDbParts = SegmentReader.listFromDir(crawlDb, config, "current")
    var crawlDbRdds: Seq[RDD[Tuple2[String, CrawlDatum]]] = Seq()
    for (part <- crawlDbParts) {
      crawlDbRdds :+= sc.sequenceFile[String, CrawlDatum](part.toString)
    }
    println("Number of CrawlDb Segments to process: " + crawlDbRdds.length)
    val crawlDbRdd = sc.union(crawlDbRdds)


    // Generate a list of segment parts
    var segParts: List[Path] = List()
    if (!segmentDir.isEmpty) {
      segParts = SegmentReader.listFromDir(segmentDir, config)
    } else if (!segmentFile.isEmpty) {
      segParts = SegmentReader.listFromFile(segmentFile)
    } else {
      println("Please provide Segment Path")
      System.exit(1)
    }
    // Converting all Segment parts to RDDs
    var segRdds: Seq[RDD[Tuple2[String, Content]]] = Seq()
    for (part <- segParts) {
      segRdds :+= sc.sequenceFile[String, Content](part.toString)
    }
    println("Number of Segments to process: " + segRdds.length)
    val segRdd = sc.union(segRdds)

    val rdd:RDD[Tuple4[String, Content, CrawlDatum, Inlinks]] = segRdd
      .join(crawlDbRdd)
      .leftOuterJoin(linkDbRdd)
      .map{case (url, ((content, crawlDatum), inLinks)) => (url, content, crawlDatum, inLinks match {
        case Some(inLinks) => inLinks
        case None => null
      })}





    /*val rdd:RDD[Tuple4[String, Content, CrawlDatum, Inlinks]] = segRdd.leftOuterJoin(crawlDbRdd).leftOuterJoin(linkDbRdd)
      .map{case (url, ((content, crawlDatum), inLinks)) => (url, content, crawlDatum match {
        case Some(crawlDatum) => crawlDatum
        case None => null
      }, inLinks match {
        case Some(inLinks) => inLinks
        case None => null
      })}*/

    

    println("Total number of URLs: " + rdd.collect().length)

    // Stop Spark Context
    sc.stop()
  }

}

object CrawlDumper extends Loggable with Serializable {

  def main(args: Array[String]) {
    new CrawlDumper().run(args)
  }

}
