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

package gov.nasa.jpl.analytics.nutch

import java.io.{FileNotFoundException, File}
import java.util.{Date, Calendar, Scanner}

import com.google.gson.Gson
import gov.nasa.jpl.analytics.base.Loggable
import gov.nasa.jpl.analytics.model.{ParsedData, CdrDumpParam, CdrV2Format}
import gov.nasa.jpl.analytics.solr.schema.FieldMapper
import gov.nasa.jpl.analytics.tika.TikaParser
import gov.nasa.jpl.analytics.util.{ParseUtil, CommonUtil, Constants}
import org.apache.commons.lang.ArrayUtils
import org.apache.commons.math3.util.Pair
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocatedFileStatus, RemoteIterator, FileSystem, Path}
import org.apache.nutch.crawl.{FetchSchedule, CrawlDatum, Inlinks, Inlink}
import org.apache.nutch.protocol.Content
import org.apache.spark.SparkContext
import org.apache.tika.metadata.Metadata
import org.joda.time.format.DateTimeFormat
import org.json.JSONObject

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by karanjeetsingh on 8/30/16.
  */
object SegmentReader extends Loggable with Serializable {

  val TEAM: String = "JPL"
  val CRAWLER: String = "Nutch 1.12"
  val VERSION: String = "2"
  val MAX_INLINKS: Int = 5000

  def filterUrl(content: Content): Boolean = {
    if (content.getContentType == null || (Constants.key.TEXT_MIME_TYPES.contains(content.getContentType) &&
      (content.getContent == null || content.getContent.isEmpty)))
      return false
    true
  }

  def filterNonImages(content: Content): Boolean = {
    if (content.getContentType == null || content.getContentType.isEmpty ||  content.getContentType.contains("image")
    || content.getContent.length < 150)
      return false
    true
  }

  def filterImages(content: Content): Boolean = {
    filterImages(content.getContentType)
  }

  def filterImages(doc: org.json.simple.JSONObject): Boolean = {
    filterImages(doc.get(Constants.key.CDR_CONTENT_TYPE).toString)
  }

  def filterImages(contentType: String): Boolean = {
    if (contentType == null || contentType.isEmpty || !contentType.contains("image")) {
      return false
    }
    true
  }

  def filterTextUrl(content: Content): Boolean = {
    if (Constants.key.TEXT_MIME_TYPES.contains(content.getContentType))
      return true
    false
  }

  def filterDocs(url: String, hashes: java.util.Map[String, String]): Boolean = {
    if (hashes.containsKey(url.trim)) {
      return true
    } else {
      println(url)
      println(hashes.size())
      return false
    }
  }

  def flatten(pair: Tuple2[String, String]): Iterable[(String, String)] = {
    //f, text = pair
    val path = pair._1.toString
    val docs: Array[String] = pair._2.split("\n")

    var flatDocs: List[(String, String)] = List()

    for (doc <- docs) {
      flatDocs :+= (path, doc)
    }

    flatDocs

    //return List(pair._1.split("\n") + (f) for line in pair._2.splitlines())
  }

  def toGson(doc: String): Map[String, Any] = {
    val gson: Gson = new Gson()
    val newGson: Map[String, Any] = gson.fromJson[Map[String, Any]](doc, Map.getClass)
    newGson
  }

  def addParentUrl(doc: org.json.simple.JSONObject, map: java.util.HashMap[String, String]): Map[String, Any] = {
    val url: String = doc.get(Constants.key.CDR_OBJ_PARENT).toString
    val hash: String = CommonUtil.hashString(url + "-" + map.get(url))
    val temp: String = doc.get(Constants.key.CDR_CRAWL_DATA).toString

    var newGson: Map[String, Any] = Map()
    val iter = doc.keySet().iterator()

    while (iter.hasNext) {
      val key: String = iter.next().toString
      if (!key.equals(Constants.key.CDR_OBJ_PARENT) && !key.equals(Constants.key.CDR_CRAWL_DATA)) {
        newGson += (key -> doc.get(key))
      }
    }

    newGson += (Constants.key.CDR_OBJ_PARENT -> hash)

    if (temp != null && !temp.isEmpty) {
      val inlinks = temp.substring(temp.indexOf('[') + 1, temp.lastIndexOf(']')).split(",")
      var inUrls: Set[String] = Set()
      for (inlink <- inlinks) {
        val hash: String = CommonUtil.hashString(inlink.trim + "-" + map.get(inlink.trim))
        inUrls += hash
      }
      val inLinksJson: JSONObject = new JSONObject()
      inLinksJson.put(Constants.key.CDR_INLINKS, inUrls.toArray)
      newGson += (Constants.key.CDR_CRAWL_DATA -> inLinksJson)
    }

    newGson
  }

  def toCdrV2(url: String, content: Content, dumpParam: CdrDumpParam): Map[String, Any] = {
    toCdrV2(url, content, dumpParam, null, null)
  }

  def toCdrV2(url: String, content: Content, dumpParam: CdrDumpParam, crawlDatum: CrawlDatum): Map[String, Any] = {
    toCdrV2(url, content, dumpParam, null, crawlDatum)
  }

  def toCdrV2(url: String, content: Content, dumpParam: CdrDumpParam, inLinks: Inlinks): Map[String, Any] = {
    toCdrV2(url, content, dumpParam, inLinks, null)
  }

  def toCdrV2(url: String, content: Content, dumpParam: CdrDumpParam, inLinks: Inlinks, crawlDatum: CrawlDatum): Map[String, Any] = {
    val gson: Gson = new Gson()
    //LOG.info("Processing URL: " + url)
    var timestamp = ""
    if (crawlDatum == null) {
      timestamp = CommonUtil.formatTimestamp(content.getMetadata.get("Date"))
    } else {
      timestamp = "" + crawlDatum.getFetchTime
    }
    //val timestamp = CommonUtil.formatTimestamp(content.getMetadata.get("Date"))
    val parsedContent: Pair[String, Metadata] = ParseUtil.parse(content)
    var cdrJson: Map[String, Any] = Map(Constants.key.CDR_ID -> CommonUtil.hashString(url + "-" + timestamp))
    //var cdrJson: Map[String, Any] = Map(Constants.key.CDR_ID -> CommonUtil.hashString(url))
    cdrJson += (Constants.key.CDR_DOC_TYPE -> dumpParam.docType)
    cdrJson += (Constants.key.CDR_CONTENT_TYPE -> content.getContentType)
    cdrJson += (Constants.key.CDR_RAW_CONTENT -> new String(content.getContent))
    cdrJson += (Constants.key.CDR_TEXT -> parsedContent.getFirst)
    cdrJson += (Constants.key.CDR_METADATA -> new JSONObject(gson.toJson(parsedContent.getSecond)).get("metadata"))
    cdrJson += (Constants.key.CDR_CRAWLER -> CRAWLER)
    cdrJson += (Constants.key.CDR_OBJ_ORIGINAL_URL -> url)
    cdrJson += (Constants.key.CDR_OBJ_STORED_URL -> CommonUtil.reverseUrl(url))
    cdrJson += (Constants.key.CDR_TEAM -> TEAM)
    cdrJson += (Constants.key.CDR_VERSION -> VERSION)
    cdrJson += (Constants.key.CDR_URL -> url)
    cdrJson += (Constants.key.CDR_CRAWL_TS -> timestamp)

    if (content.getContentType.contains("image")) {
      // Get InLinks from LinkDB
      if (inLinks != null) {
        val iterator: Iterator[Inlink] = inLinks.iterator().asScala
        var inUrls: Set[String] = Set()
        if (iterator.hasNext) {
          val parentUrl: String = iterator.next().getFromUrl
          //val parentUrl: String = CommonUtil.hashString(iterator.next().getFromUrl)
          cdrJson += (Constants.key.CDR_OBJ_PARENT -> parentUrl)
          inUrls += parentUrl

          while (inUrls.size <= MAX_INLINKS && iterator.hasNext) {
            inUrls += iterator.next().getFromUrl
            //inUrls += CommonUtil.hashString(iterator.next().getFromUrl)
          }
          val inLinksJson: JSONObject = new JSONObject()
          inLinksJson.put(Constants.key.CDR_INLINKS, inUrls.toArray)
          cdrJson += (Constants.key.CDR_CRAWL_DATA -> inLinksJson)
        }
      }
    }
    cdrJson
  }

  def toCdrV2Expanded(url: String, content: Content, dumpParam: CdrDumpParam, inLinks: Inlinks): Map[String, Any] = {
    val gson: Gson = new Gson()
    //LOG.info("Processing URL: " + url)
    val timestamp = CommonUtil.formatTimestamp(content.getMetadata.get("Date"))
    val parsedContent: Pair[String, Metadata] = ParseUtil.parse(content)
    var cdrJson: Map[String, Any] = Map(Constants.key.CDR_ID -> CommonUtil.hashString(url + "-" + timestamp))
    cdrJson += (Constants.key.CDR_DOC_TYPE -> dumpParam.docType)
    cdrJson += (Constants.key.CDR_CONTENT_TYPE -> content.getContentType)
    cdrJson += (Constants.key.CDR_RAW_CONTENT -> new String(content.getContent))
    cdrJson += (Constants.key.CDR_TEXT -> parsedContent.getFirst)
    cdrJson += (Constants.key.CDR_METADATA -> new JSONObject(gson.toJson(parsedContent.getSecond)).get("metadata"))
    cdrJson += (Constants.key.CDR_CRAWLER -> CRAWLER)
    cdrJson += (Constants.key.CDR_OBJ_ORIGINAL_URL -> url)
    cdrJson += (Constants.key.CDR_OBJ_STORED_URL -> CommonUtil.reverseUrl(url))
    cdrJson += (Constants.key.CDR_TEAM -> TEAM)
    cdrJson += (Constants.key.CDR_VERSION -> VERSION)
    cdrJson += (Constants.key.CDR_URL -> url)
    cdrJson += (Constants.key.CDR_CRAWL_TS -> timestamp)

    // Expanded Version
    cdrJson += (Constants.key.CDR_HOST -> CommonUtil.getHost(url))

    // Get InLinks from LinkDB
    if (inLinks != null) {
      val iterator: Iterator[Inlink] = inLinks.iterator().asScala
      var inUrls: Set[String] = Set()
      if (iterator.hasNext) {
        val parentUrl: String = iterator.next().getFromUrl
        cdrJson += (Constants.key.CDR_OBJ_PARENT -> parentUrl)
        inUrls += parentUrl

        while (inUrls.size <= MAX_INLINKS && iterator.hasNext) {
          inUrls += iterator.next().getFromUrl
        }
        val inLinksJson: JSONObject = new JSONObject()
        inLinksJson.put(Constants.key.CDR_INLINKS, inUrls.toArray)
        cdrJson += (Constants.key.CDR_CRAWL_DATA -> inLinksJson)
      }
    }
    cdrJson
  }

  def timeToStr(epochMillis: Long): String =
    DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss").print(epochMillis)

  // Converts into Sparkler Schema Format
  def toSparkler(url: String, content: Content, crawlDatum: CrawlDatum, inLinks: Inlinks, crawlId: String): Map[String, Any] = {
    val gson: Gson = new Gson()
    val fetchTimestamp = crawlDatum.getFetchTime
    val id = CommonUtil.hashString(url + "-" + crawlId + "-" + fetchTimestamp)
    val parser: TikaParser = TikaParser.getInstance
    var lastModifiedTime = "0"
    try {
      val pst: String = crawlDatum.getMetaData.get("pst").toString
      lastModifiedTime = pst.split("=")(1)
    } catch {
      case e: Exception => {
      }
    }

    var sparklerJson: Map[String, Any] = Map()
    sparklerJson += (Constants.key.SPKLR_ID -> id)
    sparklerJson += (Constants.key.SPKLR_CONTENT_TYPE -> content.getContentType)
    sparklerJson += (Constants.key.SPKLR_CRAWLER -> CRAWLER)
    sparklerJson += (Constants.key.SPKLR_CRAWL_ID -> crawlId)

    sparklerJson += (Constants.key.SPKLR_URL -> url)
    sparklerJson += (Constants.key.SPKLR_FETCH_TIMESTAMP -> CommonUtil.toSolrTimestamp(fetchTimestamp))

    if (filterTextUrl(content)) {
      sparklerJson += (Constants.key.SPKLR_RAW_CONTENT -> new String(content.getContent))
      val parsedContent: ParsedData = ParseUtil.parseContent(url, content)

      // Get Extracted Metadata + Flatten
      var mdFields: Map[String, AnyRef] = Map()
      for (name: String <- parsedContent.metadata.names()) {
        mdFields += (name -> (if (parsedContent.metadata.isMultiValued(name)) parsedContent.metadata.getValues(name) else parsedContent.metadata.get(name)))
      }
      val fieldMapper: FieldMapper = FieldMapper.initialize()
      val mappedMdFields: mutable.Map[String, AnyRef] = fieldMapper.mapFields(mdFields.asJava, true).asScala
      mappedMdFields.foreach{case (k, v) => {
        var key: String = k
        if (!k.endsWith(Constants.key.MD_SUFFIX)) {
          key = k + Constants.key.MD_SUFFIX
        }
        sparklerJson += (key -> v)
      }}
      
      sparklerJson += (Constants.key.SPKLR_EXTRACTED_TEXT -> parsedContent.plainText)
      sparklerJson += (Constants.key.SPKLR_OUTLINKS -> parser.getOutlinks(content))
    }

    sparklerJson += (Constants.key.SPKLR_STATUS_NAME -> CommonUtil.statusMap.get(CrawlDatum.statNames.get(crawlDatum.getStatus)))
    sparklerJson += (Constants.key.SPKLR_STATUS_CODE -> crawlDatum.getMetaData.get("nutch.protocol.code"))

    sparklerJson += (Constants.key.SPKLR_SCORE -> crawlDatum.getScore)
    sparklerJson += (Constants.key.SPKLR_HOSTNAME -> CommonUtil.getHost(url))

    sparklerJson += (Constants.key.SPKLR_GROUP -> CommonUtil.getHost(url))

    sparklerJson += (Constants.key.SPKLR_MODIFIED_TIME -> CommonUtil.toSolrTimestamp(lastModifiedTime.toLong))

    sparklerJson += (Constants.key.SPKLR_RETRIES_SINCE_FETCH -> crawlDatum.getRetriesSinceFetch)


    sparklerJson += (Constants.key.SPKLR_RETRY_INTERVAL_SECONDS -> crawlDatum.getFetchInterval)
    sparklerJson += (Constants.key.SPKLR_RETRY_INTERVAL_DAYS -> (crawlDatum.getFetchInterval / FetchSchedule.SECONDS_PER_DAY))
    sparklerJson += (Constants.key.SPKLR_SIGNATURE -> CommonUtil.hashString(new String(content.getContent)))
    sparklerJson += (Constants.key.SPKLR_VERSION -> "1.0")

    // Get InLinks from LinkDB
    if (inLinks != null) {
      val iterator: Iterator[Inlink] = inLinks.iterator().asScala
      var inUrls: Set[String] = Set()
      if (iterator.hasNext) {
        val parentUrl: String = iterator.next().getFromUrl
        sparklerJson += (Constants.key.SPKLR_OBJ_PARENT -> parentUrl)
        inUrls += ('"' + parentUrl + '"')

        while (inUrls.size <= MAX_INLINKS && iterator.hasNext) {
          inUrls += ('"' + iterator.next().getFromUrl + '"')
        }
        val inLinksJson: JSONObject = new JSONObject()
        sparklerJson += (Constants.key.SPKLR_INLINKS -> inUrls.asJava)
      }
    }

    //sparklerJson += (Constants.key.SPKLR_OUTLINKS -> parsedContent.outlinks)

    // crawler_discover_depth and crawler_fetch_depth ??

    sparklerJson += (Constants.key.SPKLR_OBJ_RELATIVE_PATH -> CommonUtil.reverseUrl(url))
    sparklerJson += (Constants.key.SPKLR_INDEXED_AT -> CommonUtil.toSolrTimestamp(Calendar.getInstance().getTimeInMillis))

    sparklerJson
  }

  def getUrl(sc: SparkContext, segmentPath: String): Array[String] = {
    val partRDD = sc.sequenceFile[String, Content](segmentPath)
    val filteredRDD = partRDD.filter({case(text, content) => filterUrl(content)})
    val urlRDD = filteredRDD.map({case(text, content) => text})
    urlRDD.collect()
  }

  def getPlainText(sc: SparkContext, segmentPath: String): Array[(String, String)] = {
    val partRDD = sc.sequenceFile[String, Content](segmentPath)
    val filteredRDD = partRDD.filter({case(text, content) => filterTextUrl(content)})
    val cdrRDD = filteredRDD.map({case(text, content) => (text, ParseUtil.parse(content).getFirst)})
    cdrRDD.collect()
  }

  def listFromDir(segmentDir: String, config: Configuration, subDir: String): List[Path] = {
    var parts: List[Path] = List()
    val partPattern: String = ".*" + File.separator + subDir +
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
    parts
  }

  def listFromDir(segmentDir: String, config: Configuration): List[Path] = {
    listFromDir(segmentDir, config, Content.DIR_NAME)
  }

  def listDumpDir(segmentDir: String, config: Configuration): List[Path] = {
    var parts: List[Path] = List()
    val partPattern: String = ".*" + File.separator + "part-[0-9]{5}"
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
    parts
  }

  def listFromFile(segmentFile: String): List[Path] = {
    var parts: List[Path] = List()
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
    parts
  }

  // Deprecated. Left for future use if any.
  @Deprecated
  def toCdrV2Format(url: String, content: Content): CdrV2Format = {
    val cdrV2Format: CdrV2Format = new CdrV2Format()
    cdrV2Format.id = url
    cdrV2Format.contentType = content.getContentType
    cdrV2Format.rawContent = new String(content.getContent)
    cdrV2Format.crawler = CRAWLER
    cdrV2Format.objOriginalUrl = url
    cdrV2Format.team = TEAM
    cdrV2Format.version = VERSION
    cdrV2Format.crawlTimestamp = CommonUtil.formatTimestamp(content.getMetadata.get("Date"))
    cdrV2Format
  }
}

