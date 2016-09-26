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

import com.google.gson.Gson
import gov.nasa.jpl.analytics.base.Loggable
import gov.nasa.jpl.analytics.model.CdrV2Format
import gov.nasa.jpl.analytics.util.{ParseUtil, CommonUtil, Constants}
import org.apache.commons.math3.util.Pair
import org.apache.hadoop.fs.Path
import org.apache.nutch.protocol.Content
import org.apache.spark.SparkContext
import org.apache.tika.metadata.Metadata
import org.json.JSONObject

/**
  * Created by karanjeetsingh on 8/30/16.
  */
object SegmentReader extends Loggable with Serializable {

  val TEAM: String = "JPL"
  val CRAWLER: String = "Nutch 1.12"
  val VERSION: String = "2"

  //TODO: If content type is image, get inLinks
  def getCdrV2Format(sc: SparkContext, segmentPath: Path): Array[Map[String, Any]] = {
    val partRDD = sc.sequenceFile[String, Content](segmentPath.toString)
    val filteredRDD = partRDD.filter({case(text, content) => filterUrl(content)})
    val cdrRDD = filteredRDD.map({case(text, content) => (toCdrV2(text, content))})
    //println(partRDD.first()._1.toString)
    //println(partRDD.first()._2.getContentType)
    cdrRDD.collect()
  }

  def filterUrl(content: Content): Boolean = {
    if (content.getContent == null || content.getContent.isEmpty || content.getContent.length < 150)
      return false
    true
  }

  def filterTextUrl(content: Content): Boolean = {
    if (content.getContentType.contains("text") || content.getContentType.contains("ml"))
      return true
    false
  }

  def toCdrV2(url: String, content: Content): Map[String, Any] = {
    val gson: Gson = new Gson()
    val timestamp = CommonUtil.formatTimestamp(content.getMetadata.get("Date"))
    val parsedContent: Pair[String, Metadata] = ParseUtil.parse(content)
    var cdrJson: Map[String, Any] = Map(Constants.key.CDR_ID -> CommonUtil.hashString(url + "-" + timestamp))
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
    cdrJson
  }

  def getPlainText(sc: SparkContext, segmentPath: Path): Array[(String, String)] = {
    val partRDD = sc.sequenceFile[String, Content](segmentPath.toString)
    val filteredRDD = partRDD.filter({case(text, content) => filterTextUrl(content)})
    val cdrRDD = filteredRDD.map({case(text, content) => (text, ParseUtil.parse(content).getFirst)})
    cdrRDD.collect()
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

