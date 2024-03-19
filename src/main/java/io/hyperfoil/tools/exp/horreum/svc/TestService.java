package io.hyperfoil.tools.exp.horreum.svc;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.hyperfoil.tools.exp.horreum.pasted.JsonBinaryType;
import io.hyperfoil.tools.exp.horreum.entity.Label;
import io.hyperfoil.tools.exp.horreum.entity.Test;
import io.hyperfoil.tools.exp.horreum.entity.extractor.JsonpathExtractor;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import org.hibernate.query.NativeQuery;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/api/labels")
@Produces(APPLICATION_JSON)
public class TestService {


    //this is only on TestService because that is where we stored Labels for the PoC
    //TODO this does not take into account iterating
    public void calculateLabelValues(Test test,Long runId) {
        Map<Boolean, List<Label>> splitJsonpath = test.labels.stream().collect(Collectors.groupingBy(Label::usesOnlyJsonpathExtractor));
        splitJsonpath.getOrDefault(Boolean.TRUE, List.of()).forEach(label -> {
                System.out.println("jsonpath only label = " + label);
                if (label.reducer == null) { // identity reducer
                    if (label.hasForEach()) { //I don't think we support this yet but we should

                        //TODO decide how to handle labelValues from for-each
                        //get the extracted values
                        List found = Label.getEntityManager().createNativeQuery("""
                                            with found as (select r.id as run_id, l.id as label_id,e.name as name, jsonb_path_query_array(r.data,e.jsonpath::::jsonpath) as data from extractor e join label l on e.parent_id = l.id, run r where r.id = :runId and l.id = :labelId),
                                            reduced as (select run_id,label_id,name,(case when jsonb_array_length(data) > 1 then data else data->0 end) as data from found)
                                            select label_id,run_id,coalesce(jsonb_object_agg(name,data)FILTER (WHERE name IS NOT NULL), '{}'::::jsonb) as data from reduced group by run_id,label_id
                                        """).setParameter("runId", runId)
                                .setParameter("labelId", label.id)
                                .unwrap(NativeQuery.class)
                                .addScalar("label_id", Long.class)
                                .addScalar("run_id", Long.class)
                                .addScalar("data", JsonBinaryType.class)
                                .getResultList();

                        if (found.size() == 1) {
                            Object[] entry = (Object[]) found.get(0);
                            Long label_id = (Long) entry[0];
                            Long run_id = (Long) entry[1];
                            ObjectNode labelValues = (ObjectNode) entry[2];
                            //we need to decide (or have the user decide) what to do when more than 1 is for-each
                            //by default do we create N x N combinations or max(N) entries, if max(N) do we repeat previous N if other for-each are less than the current one?
                            //what about scalars? Do we repeat them per entry or only on N[0]...


                            System.out.println("TODO: IMPLEMENT THIS BIT");

                        } else {
                            //how did we get more than 1 result when filtering on runId and labelId?
                            System.out.println("TODO: FIX THiS POINTY BIT");
                        }
                        //no for each, single select put directly into db
                    } else if (label.extractors.size() == 1) {
                        //insert each extractor value directly into db
                        Label.getEntityManager().createNativeQuery("""
                                    with found as (select r.id as run_id, l.id as label_id, jsonb_path_query_array(r.data,e.jsonpath::::jsonpath) as data from extractor e join label l on e.parent_id = l.id, run r where r.id = :runId and l.id = :labelId)
                                    insert into label_values (iterated,label_id,run_id,data)
                                    select false,label_id,run_id,(case when jsonb_array_length(data) > 1 then data else data->0 end) from found
                                """).setParameter("runId", runId).setParameter("labelId", label.id).executeUpdate();
                    } else {
                        //combine the extractor values into a json object by name and insert it directly into the db
                        Label.getEntityManager().createNativeQuery("""
                                    with found as (select r.id as run_id, l.id as label_id,e.name as name, jsonb_path_query_array(r.data,e.jsonpath::::jsonpath) as data from extractor e join label l on e.parent_id = l.id, run r where r.id = :runId and l.id = :labelId),
                                    reduced as (select run_id,label_id,name,(case when jsonb_array_length(data) > 1 then data else data->0 end) as data from found)
                                    insert into label_values(iterated,label_id,run_id,data)
                                    select false,label_id,run_id,coalesce(jsonb_object_agg(name,data)FILTER (WHERE name IS NOT NULL), '{}'::::jsonb) as data from reduced group by run_id,label_id
                                """).setParameter("runId", runId).setParameter("labelId", label.id).executeUpdate();
                    }
                } else {
                    //TODO we need to execute a reducer
                }

        });
        splitJsonpath.getOrDefault(Boolean.FALSE, List.of()).stream().sorted().forEach(label -> {
            System.out.println("mixed label = " + label);
            if (label.reducer == null) {
                if (label.extractors.size() == 1) {
                    System.out.println("  size == 1");
                    //TODO I think this incorrectly assumes the extractor is a labelValueExtactor, it could be metadata
                    //insert each extractor value directly into db
                    Label.getEntityManager().createNativeQuery("""
                                with found as (select r.id as run_id, l.id as label_id, jsonb_path_query_array(lv.data,e.jsonpath::::jsonpath) as data from extractor e join label l on e.parent_id = l.id join label_values lv on lv.label_id = e.target_id and lv.run_id = :runId, run r where l.id = :labelId)
                                insert into label_values(iterated,label_id,run_id,data)
                                select false,label_id,run_id,(case when jsonb_array_length(data) > 1 then data else data->0 end) from found
                            """).setParameter("runId", runId).setParameter("labelId", label.id).executeUpdate();
                }else{

                }
            }
        });
    }
}
