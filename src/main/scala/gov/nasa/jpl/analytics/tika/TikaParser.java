package gov.nasa.jpl.analytics.tika;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by karanjeetsingh on 1/9/17.
 */
public class TikaParser {

    public final Logger LOG = LoggerFactory.getLogger(TikaParser.class);
    private ParseUtil parseUtil;
    private static TikaParser INSTANCE;

    public TikaParser() {
        try {
            //String nutchHome = System.getProperty("nutch.home", null);
            String nutchHome = System.getenv("NUTCH_HOME");
            if (nutchHome != null) {
                LOG.info("Initializing nutch home from {}", nutchHome);
                Configuration nutchConf = NutchConfiguration.create();
                nutchConf.set("plugin.folders", new File(nutchHome, "plugins").getAbsolutePath());
                nutchConf.setInt("parser.timeout", 20);
                URLClassLoader loader = new URLClassLoader(
                        new URL[]{new File(nutchHome, "conf").toURI().toURL()},
                        nutchConf.getClassLoader());
                nutchConf.setClassLoader(loader);
                parseUtil = new ParseUtil(nutchConf);
            } else {
                LOG.warn("Nutch Home not set");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static TikaParser getInstance(){
        if (INSTANCE == null) {
            synchronized (TikaParser.class) {
                if (INSTANCE == null) {
                    INSTANCE = new TikaParser();
                }
            }
        }
        return INSTANCE;
    }

    public Set<String> getOutlinks(Content content){
        if (parseUtil == null || ParseSegment.isTruncated(content)) {
            return new HashSet<>();
        }
        Set<String> uniqOutlinks = new HashSet<>();
        try {
            ParseResult result = parseUtil.parse(content);
            if (!result.isSuccess()) {
                return new HashSet<>();
            }
            Parse parsed = result.get(content.getUrl());
            if (parsed != null) {
                Outlink[] outlinks = parsed.getData().getOutlinks();
                if (outlinks != null && outlinks.length > 0) {
                    for (Outlink outlink : outlinks) {
                        uniqOutlinks.add('"' + outlink.getToUrl() + '"');
                    }
                }
            } else {
                System.err.println("This shouldn't be happening");
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        return uniqOutlinks;
    }

}
