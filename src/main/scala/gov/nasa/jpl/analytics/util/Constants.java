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

package gov.nasa.jpl.analytics.util;

/**
 * Created by karanjeetsingh on 8/31/16.
 */
public interface Constants {

    interface key {

        String CDR_ID = "_id";
        String CDR_DOC_TYPE = "doc_type";
        String CDR_CONTENT_TYPE = "content_type";
        String CDR_CRAWL_DATA = "crawl_data";
        String CDR_CRAWLER = "crawler";
        String CDR_METADATA = "extracted_metadata";
        String CDR_TEXT = "extracted_text";
        String CDR_OBJ_PARENT = "obj_parent";
        String CDR_OBJ_ORIGINAL_URL = "obj_original_url";
        String CDR_OBJ_STORED_URL = "obj_stored_url";
        String CDR_RAW_CONTENT = "raw_content";
        String CDR_TEAM = "team";
        String CDR_CRAWL_TS = "timestamp";
        String CDR_URL = "url";
        String CDR_VERSION = "version";
        String CDR_INLINKS = "inlinks";
        String CDR_HOST = "host";
        String CDR_OUTLINKS = "outlinks";


        // Fields for Sparkler Schema
        String SPKLR_ID = "id";
        String SPKLR_CONTENT_TYPE = "content_type";
        String SPKLR_CRAWLER = "crawler";
        String SPKLR_CRAWL_ID = "crawl_id";
        String SPKLR_EXTRACTED_TEXT = "extracted_text";
        String SPKLR_EXTRACTED_METADATA = "extracted_metadata";
        String SPKLR_URL = "url";
        // Map to lastFetchedAt in Sparkler Schema
        String SPKLR_FETCH_TIMESTAMP = "fetch_timestamp";
        String SPKLR_RAW_CONTENT = "raw_content";
        String SPKLR_STATUS_NAME = "status_name";
        String SPKLR_STATUS_CODE = "status_code";
        String SPKLR_SCORE = "score";
        String SPKLR_HOSTNAME = "hostname";
        String SPKLR_MODIFIED_TIME = "modified_time";
        String SPKLR_RETRIES_SINCE_FETCH = "retries_since_fetch";
        String SPKLR_RETRY_INTERVAL_SECONDS = "retry_interval_seconds";
        String SPKLR_RETRY_INTERVAL_DAYS = "retry_interval_days";
        String SPKLR_SIGNATURE = "signature";
        String SPKLR_VERSION = "version";
        String SPKLR_INLINKS = "inlinks";
        String SPKLR_OUTLINKS = "outlinks";
        String SPKLR_CRAWLER_DISCOVER_DEPTH = "crawler_discover_depth";
        String SPKLR_CRAWLER_FETCH_DEPTH = "crawler_fetch_depth";
        String SPKLR_OBJ_RELATIVE_PATH = "obj_relative_path";
        String SPKLR_INDEXED_AT = "indexed_at";
        String SPKLR_OBJ_PARENT = "obj_parent";
        String SPKLR_GROUP = "group";


        String MD_SUFFIX = "_md";
    }

}
