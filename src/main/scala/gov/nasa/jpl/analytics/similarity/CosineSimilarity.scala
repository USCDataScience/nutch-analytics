package gov.nasa.jpl.analytics.similarity

import java.io.File

import gov.nasa.jpl.analytics.base.{Loggable, CliTool}
import gov.nasa.jpl.analytics.nutch.SegmentReader
import gov.nasa.jpl.analytics.util.{CommonUtil, ParseUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocatedFileStatus, RemoteIterator, FileSystem, Path}
import org.apache.nutch.protocol.Content
import org.apache.nutch.util.NutchConfiguration
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg.Vector
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

  @Option(name = "-kb", aliases = Array("--knowledgeBase"))
  var knowledgeBase: String = ""

  //@Option(name = "-o", aliases = Array("--outputDir"))
  //var outputDir: String = ""


  var sc: SparkContext = _

  def init(): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Cosine Similarity")
      .setMaster(sparkMaster)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.classesToRegister", "org.apache.nutch.protocol.Content")
    sc = new SparkContext(conf)
  }

  def printKb(tuple: Tuple2[String, Int]): Unit = {
    println(tuple._1 + " -> " + tuple._2)
  }

  override def run(): Unit = {

    // Initialize SparkContext
    init()

    // Generate a list of segment parts
    var parts: List[Path] = List()
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
          parts = filePath :: parts
        }
      }
    }

    var docs: Array[(String, String)] = Array()
    for (part <- parts) {
      docs ++= (SegmentReader getPlainText(sc, part))
      println("Processed " + part.toString)
    }

    //val m = sc.textFile(knowledgeBase)
      //.map({case doc => (doc.split("\\t")(0), doc.split("\\t").length)})

    //m.collect().foreach(printKb)

    val modelRDD = sc.textFile(knowledgeBase)
      .map({case doc => (doc.toString.split("\\t")(0), doc.toString.split("\\t")(1))})
      .map({case (url, text) => (url, ParseUtil.tokenize(text))})

    val tf: JavaRDD[Vector] = new HashingTF().transform(modelRDD.map({case (url, terms) => terms}))
    //println("TF - " + tf.first().toString)
    //println("TF - " + tf.take(2).toString)
    tf.cache()
    val idf = new IDF().fit(tf)

    //println("IDF - " + idf.idf.toString)

    val kbRDD = modelRDD
      .map({case (url, terms) => (url, new HashingTF().transform(terms))})
      .map({case (url, vectors) => (url, idf.transform(vectors))})


    val docsRDD = sc.parallelize(docs)
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
    println(cosineList.take(10).foreach(println))
    println()
    println("Last URLs: ")
    println(cosineList.takeRight(10).foreach(println))

    // Stopping Spark
    sc.stop()
  }
}


object CosineSimilarity extends Loggable with Serializable {

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
