package gov.nasa.jpl.analytics.model;

import org.apache.tika.metadata.Metadata;

import java.util.Set;

/**
 * Created by karanjeetsingh on 12/1/16.
 */
public class ParsedData {
    public String plainText;
    public Metadata metadata;
    public Set<String> outlinks;
}
