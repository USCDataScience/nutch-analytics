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
import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.nutch.protocol.Content;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by karanjeetsingh on 9/1/16.
 */
public class ParseUtil {

    private static Logger LOG = LoggerFactory.getLogger(ParseUtil.class);
    private static Splitter spaceSplitter = Splitter.on(' ').omitEmptyStrings().trimResults();

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
