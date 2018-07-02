package org.hobbit.smlbenchmark_v2.benchmark.generator.rdf.schemas;

import org.apache.jena.ontology.OntModel;
import org.apache.jena.ontology.OntModelSpec;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

public class DatAcron {

    /**
     * The namespace of the vocabulary as a string
     */
    public static final String uri ="http://www.datacron-project.eu/datAcron#";
    public static final String url = "src/main/resources/TopDatAcronOnto_SSN_FM.owl";

    /** returns the URI for this schema
     * @return the URI for this schema
     */
    public static String getURI() {
        return uri;
    }
    public static String getURL() {
        return url;
    }

    public static final OntModel m = Init.Model();

    public static final Resource VESSEL = m.createResource(uri + "Vessel" );
    public static final Resource INSTANT = m.createResource(uri + "Instant" );
    public static final Resource TRAJECTORY = m.createResource(uri + "Trajectory" );
    //public static final Resource TRAJECTORY_PART = m.createResource(uri + "TrajectoryPart" );
    public static final Resource RAW_POSITION = m.createResource(uri + "RawPosition" );


    //public static final Property HAS_DATA_VALUE = m.createProperty(uri, "hasDataValue" );
    public static final Property HAS_DRAUGHT = m.getDatatypeProperty(uri+"has_draught");
    public static final Property HAS_SPEED = m.getDatatypeProperty(uri+ "hasSpeed" );
    public static final Property HAS_TEMPORAL_FEATURE = m.getObjectProperty(uri+ "hasTemporalFeature" );
    public static final Property HAS_LONGITUDE = m.getDatatypeProperty(uri+"hasLongitude" );
    public static final Property HAS_LATTITUDE = m.getDatatypeProperty(uri + "hasLattitude" );

    public static final Property HAS_DIRECTION = m.getDatatypeProperty(uri+ "hasDirection" );
    public static final Property HAS_HEADING = m.getDatatypeProperty(uri+ "hasHeading" );
    public static final Property HAS_DEPARTURE = m.getDatatypeProperty(uri+ "hasDeparture" );

    //public static final Property SHIP_TYPE_DESC = m.getDatatypeProperty(uri+ "shipTypeDescription");
    public static final Property SHIP_TYPE_DESC = m.createDatatypeProperty(uri+ "shipTypeDescription");

    public static final Property HAS_SPATIAL_FEATURE = m.createProperty(uri, "hasSpatialFeature" );
    public static final Property REFERS_TO = m.createProperty(uri, "refersTo");

    public static class Init {
        public static OntModel Model(){
            OntModel ret =  ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM);
            ret.read(getURL(), "RDF/XML");
            return ret;
        }

    }
}