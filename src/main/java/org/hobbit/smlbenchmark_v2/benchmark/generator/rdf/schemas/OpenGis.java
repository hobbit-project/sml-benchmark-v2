package org.hobbit.smlbenchmark_v2.benchmark.generator.rdf.schemas;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

public class OpenGis {
    public static final String uri ="http://www.opengis.net/ont/sf#";
    public static final String url ="http://www.opengis.net/ont/sf";

    /** returns the URI for this schema
     * @return the URI for this schema
     */
    public static String getURL() {
        return url;
    }
    public static String getURI() {
        return uri;
    }

    private static final Model m = ModelFactory.createDefaultModel();
    public static final Resource POINT = m.createProperty(uri, "Point" );
}
