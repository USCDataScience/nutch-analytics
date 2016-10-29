package gov.nasa.jpl.analytics.tools.similarity

import java.io._

import gov.nasa.jpl.analytics.base.{Loggable, CliTool}
import gov.nasa.jpl.analytics.nutch.SegmentReader
import gov.nasa.jpl.analytics.util.{ParseUtil, CommonUtil}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.nutch.protocol.Content
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.kohsuke.args4j.Option

import scala.collection.JavaConverters._
import scala.io.Source

/**
  * Created by karanjeetsingh on 10/26/16.
  */
class RelevantHost extends CliTool {

  import RelevantHost._

  @Option(name = "-m", aliases = Array("--master"))
  var sparkMaster: String = "local[*]"

  @Option(name = "-s", aliases = Array("--segmentDir"))
  var segmentDir: String = ""

  @Option(name = "-f", aliases = Array("--segmentFile"))
  var segmentFile: String = ""

  @Option(name = "-kb", aliases = Array("--knowledgeBase"))
  var knowledgeBase: String = ""

  @Option(name = "-o", aliases = Array("--outputDir"))
  var outputDir: String = ""


  def init(): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Cosine Similarity")
      .setMaster(sparkMaster)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.classesToRegister", "org.apache.nutch.protocol.Content")
      .set("spark.kryoserializer.buffer.max", "2040m")
    sc = new SparkContext(conf)

    for (line <- Source.fromURL(getClass.getClassLoader.getResource(STOP_WORDS_FILE))) {
      if (!line.toString.trim.isEmpty) {
        stopWords += line.toString.trim.toLowerCase()
      }
    }
  }

  override def run(): Unit = {

    // Initialize SparkContext
    init()
    val config: Configuration = sc.hadoopConfiguration

    // Check & Create Output Directory
    //CommonUtil.makeSafeDir(outputDir)


    // Generating Model
    val modelRDD = sc.textFile(knowledgeBase)
      .map({case (doc) => (doc.split("\\|_\\|")(0), doc.split("\\|_\\|")(1))})

    val numDocs = modelRDD.count()

    // TF
    val tfRDD = modelRDD.flatMap{case (url, text) => ParseUtil.tokens(text)}
      .filter(!stopWords.contains(_))
      .map(word => (word.toLowerCase, 1.0))
      .reduceByKey(_ + _)

    tfRDD.take(5).foreach(println)

    // IDF
    val tempRDD = modelRDD.map{case (url, text) => (url, ParseUtil.tokens(text))}
    val idfRDD = tempRDD.flatMap(tempRDD => tempRDD._2.map(y => (y.toLowerCase, tempRDD._1)))
      .distinct()
      .filter{case (term, url) => !stopWords.contains(term)}
      .map{case (term, url) => (term, 1)}
      .reduceByKey(_ + _)
      .map{case (term, count) => (term, math.log((1.0 * numDocs)/count))}

    //println(" IDF Length: " + idfRDD.collect().length)
    //idfRDD.collect().foreach(println)

    // TF-IDF
    val tfidfRDD = tfRDD.union(idfRDD).reduceByKey(_ * _)
    val tfidf: Map[String, Double] = tfidfRDD.collect().toMap

    //println("TF-IDF Length: " + idfRDD.collect().length)
    //tfidf.take(10).foreach(println)

    // Knowledge Base Terms Magnitude
    val kbMagnitude: Double = math.sqrt(tfidfRDD.values.map(value => value * value).sum)
    println("Magnitude: " + kbMagnitude)
    val attr: SimilarityAttributes = new SimilarityAttributes(kbMagnitude, tfidf)



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
    println("Steps: Number of Segments to process: " + rdds.length)

    // Union of all RDDs
    val segRDD:RDD[Tuple2[String, Content]] = sc.union(rdds)
    println("Steps: Union of RDDs")

    // Filter for Text URLs
    val filteredRDD = segRDD.filter({case(text, content) => SegmentReader.filterTextUrl(content)})
      //.reduceByKey((key1, key2) => key1)
    println("Steps: Filtered URLs")

    // Parsing Documents
    val parsedRDD = filteredRDD.map({case(text, content) => (text, ParseUtil.parse(content).getFirst)})
    println("Steps: Parsing Documents")

    // Calculating Term Frequency for each URL
    val docsRDD = parsedRDD
      .map{case (url, text) => (url, ParseUtil.tokens(text))}
      .map{case (url, terms) => (url, dotProduct(terms, attr))}
      //.sortBy(_._2, false)
      .map{case (url, score) => (CommonUtil.getHost(url), url, score)}

    docsRDD.saveAsTextFile(outputDir)

    sc.stop()


  }
}


object RelevantHost extends Loggable with Serializable {

  var sc: SparkContext = _
  var stopWords: Set[String] = Set()
  val STOP_WORDS_FILE: String = "stop-words.txt";

  /**
    * Used for reading/writing to database, files, etc.
    * Code From the book "Beginning Scala"
    * http://www.amazon.com/Beginning-Scala-David-Pollak/dp/1430219890
    */
  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
    try { f(param) } finally { param.close() }

  def appendToFile(filename: String, data: String) =
    using(new FileWriter(filename, true)) {
      fileWriter => using(new PrintWriter(fileWriter)) {
        printWriter => printWriter.println(data)
      }
    }

  def dotProduct(docVector: Array[String], attr: SimilarityAttributes): Double = {
    var product: Double = 0.0

    // Term Frequency
    val docMap: Map[String, Double] = docVector.groupBy(identity)
      .mapValues(_.size)
      .map{ case (key, value) => key.toLowerCase -> value.asInstanceOf[Double] }

    // Intersection
    val commonTerms: Set[String] = docMap.keySet.intersect(attr.tfidf.keySet)

    commonTerms.foreach{
      item => {
        product += (docMap(item) * attr.tfidf(item))
      }
    }

    // Magnitude
    if (product != 0.0) {
      val docMagnitude: Double = math.sqrt(docMap.values
        .map(value => value * value).sum)
      product = product / (docMagnitude * attr.kbMagnitude)
    }

    product
  }

  def main(args: Array[String]) {
    new RelevantHost().run(args)
  }

}
