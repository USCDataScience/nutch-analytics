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

package gov.nasa.jpl.analytics.dump

import java.io.{PrintWriter, File}

import gov.nasa.jpl.analytics.base.{Loggable, CliTool}
import gov.nasa.jpl.analytics.nutch.SegmentReader
import gov.nasa.jpl.analytics.util.Constants
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocatedFileStatus, RemoteIterator, FileSystem, Path}
import org.apache.nutch.protocol.Content
import org.apache.nutch.util.NutchConfiguration
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

  @Option(name = "-l", aliases = Array("--linkDb"))
  var linkDb: String = ""

  @Option(name = "-o", aliases = Array("--outputDir"))
  var outputDir: String = ""


  var sc: SparkContext = _

  def init(): Unit = {
    val conf = new SparkConf()
    conf.setAppName("CDRv2Dumper")
      .setMaster(sparkMaster)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.classesToRegister", "org.apache.nutch.protocol.Content")
    sc = new SparkContext(conf)
  }

  override def run(): Unit = {

    // Initialize SparkContext
    init()

    // Generate a list of segment parts
    var parts: List[String] = List()
    val nutchConfig: Configuration = NutchConfiguration.create()
    val partPattern: String = ".*" + File.separator + Content.DIR_NAME +
      File.separator + "part-[0-9]{5}" + File.separator + "data"
    val fs: FileSystem = FileSystem.get(nutchConfig)
    val segmentDirPath: Path = new Path(segmentDir.toString)
    val segmentFiles: RemoteIterator[LocatedFileStatus] = fs.listFiles(segmentDirPath, true)
    while (segmentFiles.hasNext) {
      val next: LocatedFileStatus = segmentFiles.next()
      if (next.isFile) {
        val filePath: Path = next.getPath
        if (filePath.toString.matches(partPattern)) {
          parts = filePath.toString :: parts
        }
      }
    }

    var docs: Array[Map[String, Any]] = Array()
    for (part <- parts) {
      docs ++= SegmentReader.getCdrV2Format(sc, part)
      println("Processed " + part.toString)
    }

    val cdrRDD = sc.parallelize(docs)
      .map(doc => (doc.get(Constants.key.CDR_ID).toString, doc))
      .reduceByKey((key1, key2) => key1)
      .map({case(url, doc) => doc})

    //cdrRDD.collect().foreach(printJson)
    cdrRDD.collect().foreach(writeJson)

    sc.stop()
  }

  def writeJson(map: Map[String, Any]): Unit = {
    val obj:JSONObject = new JSONObject(map)
    new PrintWriter(outputDir + File.separator + "out.json") { write(obj.toJSONString); close}
  }

}

object Cdrv2Dump extends Loggable with Serializable {

  def printJson(map: Map[String, Any]): Unit = {
    val obj:JSONObject = new JSONObject(map)
    println(obj.toJSONString)
  }

  def main(args: Array[String]) {
    new Cdrv2Dump().run(args)
  }

}
