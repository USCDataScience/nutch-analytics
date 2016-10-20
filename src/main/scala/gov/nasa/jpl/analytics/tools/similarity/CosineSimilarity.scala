package gov.nasa.jpl.analytics.tools.similarity

import java.io.{File, PrintWriter, FileWriter}

import gov.nasa.jpl.analytics.base.{Loggable, CliTool}
import gov.nasa.jpl.analytics.nutch.SegmentReader
import gov.nasa.jpl.analytics.util.{CommonUtil, ParseUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.nutch.protocol.Content
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.kohsuke.args4j.Option

/**
  * Created by karanjeetsingh on 9/25/16.
  */
class CosineSimilarity extends CliTool {

  import CosineSimilarity._

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


  var sc: SparkContext = _

  def init(): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Cosine Similarity")
      .setMaster(sparkMaster)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.classesToRegister", "org.apache.nutch.protocol.Content")
      .set("spark.kryoserializer.buffer.max", "2040m")
    sc = new SparkContext(conf)
  }

  def printKb(tuple: Tuple2[String, Int]): Unit = {
    println(tuple._1 + " -> " + tuple._2)
  }

  override def run(): Unit = {

    // Initialize SparkContext
    init()
    val config: Configuration = sc.hadoopConfiguration

    // Check & Create Output Directory
    CommonUtil.makeSafeDir(outputDir)

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
    val filteredRDD = segRDD.filter({case(text, content) => SegmentReader.filterTextUrl(content)})
    val parsedRDD = filteredRDD.map({case(text, content) => (text, ParseUtil.parse(content).getFirst)})

    val modelRDD = sc.textFile(knowledgeBase)
      .map({case doc => (doc.toString.split("|_|")(0), ParseUtil.tokenize(doc.toString.split("|_|")(1)))})

    val tf: JavaRDD[Vector] = new HashingTF().transform(modelRDD.map({case (url, terms) => terms}))
    //println("TF - " + tf.first().toString)
    //println("TF - " + tf.take(2).toString)
    tf.cache()
    val idf = new IDF().fit(tf)

    //println("IDF - " + idf.idf.toString)

    val kbRDD = modelRDD
      .map({case (url, terms) => (url, new HashingTF().transform(terms))})
      .map({case (url, vectors) => (url, idf.transform(vectors))})


    val docsRDD = parsedRDD
      .reduceByKey((key1, key2) => key1)
      .map({case (url, text) => (url, ParseUtil.tokenize(text))})
      //.map({case (url, vectors) => (url, CommonUtil.termFrequency(vectors))})
      .map({case (url, terms) => (url, new HashingTF().transform(terms))})
      .map({case (url, vectors) => (url, idf.transform(vectors))})

    val product = kbRDD.cartesian(docsRDD)
      .map({case ((kbUrl, kbVector), (docUrl, docVector)) => ((docUrl), dotProduct(docVector, kbVector))})
      .mapValues((_, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues{ case (sum, count) => (1.0 * sum) / count }
      .sortBy(_._2, false)

    val cosineList: Array[(String, Double)] = product.collect()

    println("Top URLs: ")
    cosineList.take(20).foreach(println)
    cosineList.take(20).foreach({case (url, score) => appendToFile(outputDir + File.separator + "top.txt", url)})
    println()
    println("Bottom URLs: ")
    cosineList.takeRight(20).foreach(println)
    cosineList.takeRight(20).foreach({case (url, score) => appendToFile(outputDir + File.separator + "bottom.txt", url)})

    // Stopping Spark
    sc.stop()
  }
}


object CosineSimilarity extends Loggable with Serializable {

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

  def dotProduct(docVector: Vector, kbVector: Vector): Double = {
    /*if (i == 0) {
      i += 1
      println("Dot Product - " + kbVector.toJson.toString)
      println("Dot Product - " + docVector.toJson.toString)
    }*/
    val kbIndices: Array[Int] = kbVector.toSparse.indices
    val kbValues: Array[Double] = kbVector.toSparse.values
    val docIndices: Array[Int] = docVector.toSparse.indices
    val docValues: Array[Double] = docVector.toSparse.values
    var product: Double = 0.0
    var score: Double = 0.0

    for((hash, index) <- docIndices.view.zipWithIndex) {
      val kbIndex: Int = kbIndices.indexWhere(_ == hash)
      if (kbIndex != -1) {
        product += docValues(index) * kbValues(kbIndex)
      }
    }
    if (product != 0.0) {
      val docMagnitude: Double = math.sqrt(docValues.map(value => value * value).sum)
      val kbMagnitude: Double = math.sqrt(kbValues.map(value => value * value).sum)
      product = product / (docMagnitude * kbMagnitude)
    }
    product
  }

  def main(args: Array[String]) {
    new CosineSimilarity().run(args)
  }

}
