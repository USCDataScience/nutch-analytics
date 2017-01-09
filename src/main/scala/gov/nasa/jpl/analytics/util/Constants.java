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

import com.google.common.collect.Sets;

import java.util.Set;

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

        Set<String> TEXT_MIME_TYPES = Sets.newHashSet("text/asp", "text/aspdotnet", "text/calendar", "text/css", "text/csv", "text/directory", "text/dns", "text/ecmascript", "text/enriched", "text/example", "text/html", "text/iso19139+xml", "text/parityfec", "text/plain", "text/prs.fallenstein.rst", "text/prs.lines.tag", "text/red", "text/rfc822-headers", "text/richtext", "text/rtp-enc-aescm128", "text/rtx", "text/sgml", "text/t140", "text/tab-separated-values", "text/troff", "text/ulpfec", "text/uri-list", "text/vnd.abc", "text/vnd.curl", "text/vnd.curl.dcurl", "text/vnd.curl.mcurl", "text/vnd.curl.scurl", "text/vnd.dmclientscript", "text/vnd.esmertec.theme-descriptor", "text/vnd.fly", "text/vnd.fmi.flexstor", "text/vnd.graphviz", "text/vnd.in3d.3dml", "text/vnd.in3d.spot", "text/vnd.iptc.anpa", "text/vnd.iptc.newsml", "text/vnd.iptc.nitf", "text/vnd.latex-z", "text/vnd.motorola.reflex", "text/vnd.ms-mediapackage", "text/vnd.net2phone.commcenter.command", "text/vnd.si.uricatalogue", "text/vnd.sun.j2me.app-descriptor", "text/vnd.trolltech.linguist", "text/vnd.wap.si", "text/vnd.wap.sl", "text/vnd.wap.wml", "text/vnd.wap.wmlscript", "text/vtt", "text/x-actionscript", "text/x-ada", "text/x-applescript", "text/x-asciidoc", "text/x-aspectj", "text/x-assembly", "text/x-awk", "text/x-basic", "text/x-c++hdr", "text/x-c++src", "text/x-cgi", "text/x-chdr", "text/x-clojure", "text/x-cobol", "text/x-coffeescript", "text/x-coldfusion", "text/x-common-lisp", "text/x-csharp", "text/x-csrc", "text/x-d", "text/x-diff", "text/x-eiffel", "text/x-emacs-lisp", "text/x-erlang", "text/x-expect", "text/x-forth", "text/x-fortran", "text/x-go", "text/x-groovy", "text/x-haml", "text/x-haskell", "text/x-haxe", "text/x-idl", "text/x-ini", "text/x-java-properties", "text/x-java-source", "text/x-jsp", "text/x-less", "text/x-lex", "text/x-log", "text/x-lua", "text/x-matlab", "text/x-ml", "text/x-modula", "text/x-objcsrc", "text/x-ocaml", "text/x-pascal", "text/x-perl", "text/x-php", "text/x-prolog", "text/x-python", "text/x-rexx", "text/x-rsrc", "text/x-rst", "text/x-ruby", "text/x-scala", "text/x-scheme", "text/x-sed", "text/x-setext", "text/x-sql", "text/x-stsrc", "text/x-tcl", "text/x-tika-text-based-message", "text/x-uuencode", "text/x-vbasic", "text/x-vbdotnet", "text/x-vbscript", "text/x-vcalendar", "text/x-vcard", "text/x-verilog", "text/x-vhdl", "text/x-web-markdown", "text/x-yacc", "text/x-yaml", "application/xhtml+xml", "application/xml", "application/xml-dtd", "application/x-bibtex-text-file");
    }

}
