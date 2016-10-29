package gov.nasa.jpl.analytics.tools.stats

import gov.nasa.jpl.analytics.base.{CliTool, Loggable}
import gov.nasa.jpl.analytics.nutch.SegmentReader
import gov.nasa.jpl.analytics.util.CommonUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.nutch.protocol.Content
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.kohsuke.args4j.Option

/**
  * Created by karanjeetsingh on 10/13/16.
  */
class DDStats extends CliTool {

  @Option(name = "-m", aliases = Array("--master"))
  var sparkMaster: String = "local[*]"

  @Option(name = "-s", aliases = Array("--segmentDir"))
  var segmentDir: String = ""

  @Option(name = "-f", aliases = Array("--segmentFile"))
  var segmentFile: String = ""


  var sc: SparkContext = _

  def init(): Unit = {
    val conf = new SparkConf()
    conf.setAppName("DDStats")
      .setMaster(sparkMaster)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.classesToRegister", "org.apache.nutch.protocol.Content")
      .set("spark.kryoserializer.buffer.max", "2040m")
    sc = new SparkContext(conf)
  }

  override def run(): Unit = {

    // Initialize SparkContext
    init()

    // Generate a list of segment parts
    var parts: List[Path] = List()
    val config: Configuration = sc.hadoopConfiguration
    if (!segmentDir.isEmpty) {
      parts = SegmentReader.listFromDir(segmentDir, config)
    } else if (!segmentFile.isEmpty) {
      parts = SegmentReader.listFromFile(segmentFile)
    } else {
      println("Please provide Segment Path")
      System.exit(1)
    }

    // Converting all Segment parts to RDDs
    var docs: Array[String] = Array()
    var rdds: Seq[RDD[Tuple2[String, Content]]] = Seq()
    for (part <- parts) {
      rdds :+= sc.sequenceFile[String, Content](part.toString)
    }
    println("Number of Segments to process: " + rdds.length)

    // Union of all RDDs
    val segRDD:RDD[Tuple2[String, Content]] = sc.union(rdds)
    segRDD.saveAsSequenceFile("allSegments")

    // Filtering & Operations
    val filteredRDD = segRDD.filter({case(text, content) => SegmentReader.filterUrl(content)})
    val urlRDD = filteredRDD.map({case(url, content) => Some(url).get.toString}).distinct()
    val hostRDD = urlRDD.map(url => CommonUtil.getHost(url)).distinct().collect()

    urlRDD.map(url => (CommonUtil.getHost(url), 1))
      .reduceByKey(_ + _, 1)
      .map(item => item.swap)
      .sortByKey(false, 1)
      .map(item => item.swap)
      .saveAsTextFile("host-url")
    println("Number of Hosts: " + hostRDD.length)
    println("Number of Web Pages: " + urlRDD.distinct().collect().length)

    sc.stop()
  }

}

object DDStats extends Loggable with Serializable {

  def main(args: Array[String]) {
    new DDStats().run(args)
  }

}