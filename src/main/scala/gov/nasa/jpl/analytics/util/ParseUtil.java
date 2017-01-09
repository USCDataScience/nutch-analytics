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

import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;
import gov.nasa.jpl.analytics.model.ParsedData;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.nutch.protocol.Content;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.LinkContentHandler;
import org.apache.tika.sax.WriteOutContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by karanjeetsingh on 9/1/16.
 */
public class ParseUtil {

    private static Logger LOG = LoggerFactory.getLogger(ParseUtil.class);
    private static Splitter spaceSplitter = Splitter.on(' ').omitEmptyStrings().trimResults();

    public static ParsedData parseContent(String url, Content content) {
        ParsedData parsedData = new ParsedData();
        InputStream stream = new ByteArrayInputStream(content.getContent());
        LinkContentHandler linkHandler = new LinkContentHandler();
        AutoDetectParser parser = new AutoDetectParser();
        Metadata meta = new Metadata();
        WriteOutContentHandler outHandler = new WriteOutContentHandler(-1);
        BodyContentHandler contentHandler = new BodyContentHandler(outHandler);
        Set<String> outlinks = new HashSet<>();
        String plainText = "";
        /* Parsing Outlinks using Nutch Utils
        try {
            // Parse OutLinks
            meta.set("resourceName", url);
            parser.parse(stream, linkHandler, meta);
            for (Link link: linkHandler.getLinks()) {
                String outlink = link.getUri().trim();
                if (!outlink.isEmpty() && (outlink.startsWith("http") || outlink.startsWith("ftp"))) {
                    outlinks.add('"' + link.getUri().trim() + '"');
                }
            }
        } catch (Exception e) {
            LOG.warn("PARSING-OUTLINKS-ERROR");
            LOG.warn(e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(stream);
        }
        */
        try {
            meta  = new Metadata();
            // Parse Text
            stream = new ByteArrayInputStream(content.getContent());
            meta.set("resourceName", url);
            parser.parse(stream, contentHandler, meta);
            plainText = outHandler.toString();
        } catch (Exception e) {
            LOG.warn("PARSING-CONTENT-ERROR");
            LOG.warn(e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(stream);
        }

        parsedData.metadata = meta;
        parsedData.outlinks = outlinks;
        parsedData.plainText = plainText;

        return parsedData;
    }

    public static Pair<String, Metadata> parse(Content content) {
        ByteArrayInputStream stream = new ByteArrayInputStream(content.getContent());
        try {
            if ((content.getContentType().contains("text") || content.getContentType().contains("ml")) &&
                    (!content.getContentType().contains("vnd")))
                return parse(stream);
            else
                return new Pair<String, Metadata>("", new Metadata());
        } catch (Exception e) {
            LOG.error("Exception occurred at URL: " + content.getUrl());
            e.printStackTrace();
        } catch (Error e) {
            LOG.error("Error occurred at URL: " + content.getUrl());
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(stream);
        }
        // Some Exception or Unexpected Error
        return new Pair<String, Metadata>("", new Metadata());
    }

    public static Pair<String, Metadata> parse(InputStream stream) throws Exception, Error {
        Metadata metadata = new Metadata();
        //WriteOutContentHandler outHandler = new WriteOutContentHandler();
        //BodyContentHandler contentHandler = new BodyContentHandler(outHandler);
        try {
            //AutoDetectParser parser = new AutoDetectParser();
            //parser.parse(stream, contentHandler, metadata);
            //return new Pair<String, Metadata>(outHandler.toString(), metadata);
            String text = new Tika().parseToString(stream, metadata);
            return new Pair<String, Metadata>(text, metadata);
        } catch (IOException e) {
            LOG.error("IO Exception occurred while parsing content");
            e.printStackTrace();
        } catch (TikaException e) {
            LOG.error("TIKA Exception occurred while parsing content");
            e.printStackTrace();
        }
        // Some Exception or Unexpected Error
        return new Pair<String, Metadata>("", new Metadata());
    }

    public static Iterable<String> tokenize(String text) {
        text = text.replaceAll("\\n", " ");
        return spaceSplitter.split(text);
    }

    public static String[] tokens(String text) {
        text = text.replaceAll("\\n", " ");
        return FluentIterable.from(spaceSplitter.split(text)).toArray(String.class);
    }

    public static Iterator<Map<String, Integer>> generateIdf(String url, String[] terms) {

        return null;
    }

}
