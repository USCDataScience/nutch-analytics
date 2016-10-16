package gov.nasa.jpl.analytics.tools.stats

import java.io.{FileNotFoundException, File}
import java.util.Scanner

import gov.nasa.jpl.analytics.base.{CliTool, Loggable}
import gov.nasa.jpl.analytics.nutch.SegmentReader
import gov.nasa.jpl.analytics.util.{CommonUtil, Constants}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocatedFileStatus, RemoteIterator, FileSystem, Path}
import org.apache.nutch.protocol.Content
import org.apache.nutch.segment.SegmentMerger
import org.apache.nutch.util.NutchConfiguration
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

  @Option(name = "-n", aliases = Array("--nameNode"))
  var nameNode: String = "localhost"

  @Option(name = "-p", aliases = Array("--nameNodePort"))
  var nameNodePort: Integer = 50070

  @Option(name = "-f", aliases = Array("--segmentFile"))
  var segmentFile: String = ""


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

    // Generate a list of segment parts
    var parts: List[Path] = List()
    if (!segmentDir.isEmpty) {
      val config: Configuration = sc.hadoopConfiguration
      val partPattern: String = ".*" + File.separator + Content.DIR_NAME +
        File.separator + "part-[0-9]{5}" + File.separator + "data"
      val fs: FileSystem = FileSystem.get(config)
      val segmentDirPath: Path = new Path(segmentDir.toString)
      val segmentFiles: RemoteIterator[LocatedFileStatus] = fs.listFiles(segmentDirPath, true)
      while (segmentFiles.hasNext) {
        val next: LocatedFileStatus = segmentFiles.next()
        if (next.isFile) {
          val filePath: Path = next.getPath
          if (filePath.toString.matches(partPattern)) {
            parts = filePath :: parts
          }
        }
      }
    } else if (!segmentFile.isEmpty) {
      try {
        val scanner: Scanner = new Scanner(new File(segmentFile))
        while(scanner.hasNext) {
          val line: String = scanner.nextLine()
          parts = new Path(line) :: parts
        }
        scanner.close()
      } catch {
        case e: FileNotFoundException => println("Segment File Path is Wrong!!")
      }

    } else {
      println("Please provide Segment Path")
    }

    //println("Merging Segments...")
    //CommonUtil.mergeSegments(new Path("mergedSegment"), parts.toArray)
    //println("Segments are now Merged")

    var docs: Array[String] = Array()
    var rdds: Seq[RDD[Tuple2[String, Content]]] = Seq()
    for (part <- parts) {
      rdds :+= sc.sequenceFile[String, Content](part.toString)
    }
    println("Number of Segments to process: " + rdds.length)
    val segRDD:RDD[Tuple2[String, Content]] = sc.union(rdds)
    //println(segRDD.collect().length)
    val filteredRDD = segRDD.filter({case(text, content) => SegmentReader.filterUrl(content)})
    val urlRDD = filteredRDD.map({case(url, content) => Some(url).get.toString})

    val hostRDD = urlRDD.map(url => CommonUtil.getHost(url)).distinct().collect()
    println("Number of Webpages: " + urlRDD.distinct().collect().length)
    println("Number of Hosts: " + hostRDD.length)

    //  docs ++= SegmentReader.getUrl(sc, part)
    //  println("Processed " + part.toString)
    //}
    //docs ++= SegmentReader.getUrl(sc, "mergedSegment")

    //val cdrRDD = sc.parallelize(docs)
      //.map(doc => doc.get(Constants.key.CDR_URL).get.toString)
      //.reduceByKey((key1, key2) => key1)
      //.map({case(url, doc) => url})

    //val hostRDD = cdrRDD.map(url => CommonUtil.getHost(url)).distinct().collect()

    //println("Number of Webpages: " + cdrRDD.distinct().collect().length)
    //println("Number of Hosts: " + hostRDD.length)


  }

}

object DDStats extends Loggable with Serializable {

  def main(args: Array[String]) {
    new DDStats().run(args)
  }

}