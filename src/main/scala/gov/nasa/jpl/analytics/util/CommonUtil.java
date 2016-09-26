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

import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;

/**
 * Created by karanjeetsingh on 8/31/16.
 */
public class CommonUtil {

    private static Logger LOG = LoggerFactory.getLogger(CommonUtil.class);
    private static boolean SIMPLE_DATE_FORMAT = true;

    public static String formatTimestamp(String sdate) {
        if (SIMPLE_DATE_FORMAT) {
            String timestamp = null;
            try {
                long epoch = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z").parse(ifNullString(sdate)).getTime();
                timestamp = String.valueOf(epoch);
            } catch (ParseException pe) {
                LOG.warn(pe.getMessage());
            }
            return timestamp;
        } else {
            return ifNullString(sdate);
        }
    }

    public static String reverseUrl(String url) {
        String reversedURLPath = "";
        try {
            String[] reversedURL = TableUtil.reverseUrl(url).split(":");
            reversedURL[0] = reversedURL[0].replace('.', '/');

            reversedURLPath = reversedURL[0] + "/" + hashString(url);
        } catch (MalformedURLException e) {
            LOG.error("Error occurred while reversing the URL " + url);
            e.printStackTrace();
        }
        return reversedURLPath;
    }

    public static String hashString(String str) {
        String hash = "";
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(str.getBytes("UTF-8"));
            byte[] digest = md.digest();
            hash = String.format("%064x", new java.math.BigInteger(1, digest)).toUpperCase();
        } catch (NoSuchAlgorithmException e) {
            LOG.error("Not a valid Hash Algorithm for String " + str);
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            LOG.error("Not a valid Encoding for String " + str);
            e.printStackTrace();
        }
        return hash;
    }

    public static HashMap<String, Integer> termFrequency(Iterable<String> terms) {
        HashMap<String, Integer> map = new HashMap();
        for (String term: terms) {
            if (!map.containsKey(term)) {
                map.put(term, 1);
            } else {
                map.put(term, map.get(term) + 1);
            }
        }
        return map;
    }

    private static String ifNullString(String value) {
        return (value != null) ? value : "";
    }

    public static void main(String[] args) {
    }

}
