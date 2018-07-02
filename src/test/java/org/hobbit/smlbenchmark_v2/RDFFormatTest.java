package org.hobbit.smlbenchmark_v2;

import org.apache.jena.ontology.*;
import org.apache.jena.ontology.impl.OntologyImpl;
import org.apache.jena.rdf.model.*;
import org.hobbit.smlbenchmark_v2.benchmark.generator.rdf.schemas.DUL;
import org.hobbit.smlbenchmark_v2.benchmark.generator.rdf.schemas.DatAcron;
import org.hobbit.smlbenchmark_v2.benchmark.generator.rdf.schemas.OpenGis;
import org.junit.Ignore;
import org.junit.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Set;

import static org.hobbit.smlbenchmark_v2.Constants.BENCHMARK_URI;

public class RDFFormatTest {

    @Ignore
    @Test
    public void prepareModel(){

        //OntModel model = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM);
        OntModel model = DatAcron.m;

        model.setNsPrefix("datAcron", DatAcron.getURI());
        model.setNsPrefix("DUL", DUL.getURI());
        model.setNsPrefix("opengis", OpenGis.getURI());

        Individual ship = model.createIndividual(BENCHMARK_URI+ "#Ship1", DatAcron.VESSEL);
        ship.addProperty(DatAcron.SHIP_TYPE_DESC, "1");
        ship.addProperty(DatAcron.HAS_DRAUGHT, model.createTypedLiteral(1));
        ship.addProperty(DatAcron.HAS_SPEED, "1");

        Individual trajectory = model.createIndividual(BENCHMARK_URI+ "#Trajectory1", DatAcron.TRAJECTORY);
        //trajectory.
        trajectory.addProperty(DatAcron.HAS_DEPARTURE,"Departure1");
        trajectory.addProperty(DatAcron.REFERS_TO, ship);

        //time: temporalEntity
        Individual time = model.createIndividual(BENCHMARK_URI+ "#Time1", DatAcron.INSTANT);
        time.addProperty(DUL.HAS_DATA_VALUE, "timestamp");

        //sf:Point > sf: Geometry
        Individual point = model.createIndividual(BENCHMARK_URI+ "#Point1", OpenGis.POINT);
        point.addProperty(DatAcron.HAS_LONGITUDE, "long1");
        point.addProperty(DatAcron.HAS_LATTITUDE, "lat1");

        //datAcron: RawPosition > TrajectoryPart
        Individual rawPosition = model.createIndividual(BENCHMARK_URI+ "#RawPosition1", DatAcron.RAW_POSITION);
        rawPosition.addProperty(DatAcron.REFERS_TO, trajectory);
        rawPosition.addProperty(DatAcron.HAS_TEMPORAL_FEATURE, time);
        rawPosition.addProperty(DatAcron.HAS_SPATIAL_FEATURE, point);
        rawPosition.addProperty(DatAcron.HAS_HEADING, "heading1");
        rawPosition.addProperty(DatAcron.HAS_DIRECTION, "course1");

        try {
            model.write(new FileWriter("output.rdf"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        String test="123";
    }



}
