package io.hyperfoil.tools.exp.horreum.svc;

import io.hyperfoil.tools.exp.horreum.entity.Label;
import io.hyperfoil.tools.exp.horreum.entity.LabelReducer;
import io.hyperfoil.tools.exp.horreum.entity.Test;
import io.hyperfoil.tools.exp.horreum.entity.extractor.Extractor;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.*;
import org.jboss.resteasy.reactive.Separator;

import java.util.ArrayList;
import java.util.List;

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/api/test")
@Produces(APPLICATION_JSON)public class TestService {

    @Inject
    LabelService service;

    @POST
    @Transactional
    public void create(Test t){
        t.persist();
    }

    @GET
    @Path("{id}")
    public Test getById(@PathParam("id") int testId){
        return Test.findById(testId);
    }

    public Test getByName(String name){
        return Test.find("from Test t where t.name =?1",name).firstResult();
    }


    @GET
    @Path("rhivos")
    @Transactional
    public Test createRhivos(){
        Test t = new Test("rhivos-perf-comprehensive");
        String transformName = "transform";
        String transformPrefix = transformName+Extractor.FOR_EACH_SUFFIX+Extractor.NAME_SEPARATOR;//transform[]:
        t.loadLabels(
                new Label(transformName,t)
                        .loadExtractors(
                                Extractor.fromString("$.user").setName("user"),
                                Extractor.fromString("$.uuid").setName("uuid"),
                                Extractor.fromString("$.run_id").setName("run_id"),
                                Extractor.fromString("$.start_time").setName("start_time"),
                                Extractor.fromString("$.end_time").setName("end_time"),
                                Extractor.fromString("$.description").setName("description"),
                                Extractor.fromString("$.ansible_facts").setName("ansible_facts"),
                                Extractor.fromString("$.stressng_workload[*].test_results.test_config.stressors[0].workers").setName("workers"),
                                Extractor.fromString("$.stressng_workload[*].test_results.test_config.stressors[0].stressor").setName("stressor"),
                                Extractor.fromString("$.stressng_workload[*].pcp_time_series").setName("stressng_pcp_ts"),
                                Extractor.fromString("$.stressng_workload[*].sample_uuid").setName("stressng_sample_uuid"),
                                Extractor.fromString("$.coremark_pro_workload[*].pcp_time_series").setName("coremark_pro_pcp_ts"),
                                Extractor.fromString("$.coremark_pro_workload[*].sample_uuid").setName("coremark_pro_sample_uuid"),
                                Extractor.fromString("$.autobench_workload[*].pcp_time_series").setName("autobench_pcp_ts"),
                                Extractor.fromString("$.autobench_workload[*].sample_uuid").setName("autobench_sample_uuid"),
                                Extractor.fromString("$.stressng_workload[*].test_results").setName("stressng_results"),
                                Extractor.fromString("$.coremark_pro_workload[*].results").setName("coremark_pro_results"),
                                Extractor.fromString("$.autobench_workload[*].results").setName("autobench_results")
                        )
                        .setTargetSchema("urn:rhivos-perf-comprehensive-datasets:01")
                        .setReducer(new LabelReducer(
                        """
                                ({
                                    stressng_sample_uuid, coremark_pro_sample_uuid, autobench_sample_uuid,
                                    stressng_results, coremark_pro_results, autobench_results,
                                    stressng_pcp_ts, coremark_pro_pcp_ts, autobench_pcp_ts,
                                    user, uuid, run_id, start_time, end_time, description, ansible_facts, workers, stressor
                                }) => {
                                    var sngmap = stressng_sample_uuid.map((value, i) => ({
                                        sample_uuid: value,
                                        workload: "stressng",
                                        metadata: {user, uuid, run_id, start_time, end_time, description, ansible_facts},
                                        workers: workers[i],
                                        stressor: stressor[i],
                                        results: stressng_results[i],
                                        pcp_ts: stressng_pcp_ts[i]
                                    }));
                                    var cmpmap = coremark_pro_sample_uuid.map((value, i) => ({
                                        sample_uuid: value,
                                        workload: "coremark_pro",
                                        metadata: {user, uuid, run_id, start_time, end_time, description, ansible_facts},
                                        workers: coremark_pro_results[i]["coremark_pro_params"]["workers"],
                                        contexts: coremark_pro_results[i]["coremark_pro_params"]["contexts"],
                                        results: coremark_pro_results[i]["coremark_pro_results"],
                                        pcp_ts: coremark_pro_pcp_ts[i]
                                    }));
                                    var abmap = autobench_sample_uuid.map((value, i) => ({
                                        sample_uuid: value,
                                        workload: "autobench",
                                        metadata: {user, uuid, run_id, start_time, end_time, description, ansible_facts},
                                        workers: autobench_results[i]["autobench_params"]["workers"],
                                        contexts: autobench_results[i]["autobench_params"]["contexts"],
                                        results: autobench_results[i]["autobench_results"],
                                        pcp_ts: autobench_pcp_ts[i]
                                    }));
                                    const mymap = sngmap.concat(cmpmap, abmap);
                                    return mymap;
                                }
                                """
                        )),
                new Label("Autobench Multi Core",t)
                        .loadExtractors(
                                Extractor.fromString(transformPrefix+"$.results.*.MultiCore").setName("results"),
                                Extractor.fromString(transformPrefix+"$.workload").setName("workload")
                                )
                        .setReducer(new LabelReducer(
                        """
                                value => {
                                    console.log("Autobench Multi Core",value)
                                    if (value["workload"] != "autobench") {
                                        return null
                                    }
                                    if(!value["results"]) {
                                        return 0
                                    } else {
                                        return parseFloat(((value["results"].reduce((a,b) => a+b, 0))/value["results"].length).toFixed(3))
                                    }
                                };
                                """
                        )),
                new Label("Autobench Scaling",t)
                        .loadExtractors(
                                Extractor.fromString(transformPrefix+"$.results.*.Scaling").setName("results"),
                                Extractor.fromString(transformPrefix+"$.workload").setName("workload")
                        ).setReducer(new LabelReducer(
                        """
                                value => {
                                    console.log("Autobench Scaling",value)
                                    if (value["workload"] != "autobench") {
                                        return null
                                    }
                                    if(!value["results"]) {
                                        return 0
                                    } else {\s
                                        return parseFloat(((value["results"].reduce((a,b) => a+b, 0))/value["results"].length).toFixed(3))
                                    }
                                };
                                """
                        )),
                new Label("Autobench Single Core",t)
                        .loadExtractors(
                                Extractor.fromString(transformPrefix+"$.results.*.SingleCore").setName("results"),
                                Extractor.fromString(transformPrefix+"$.workload").setName("workload")
                        ).setReducer(new LabelReducer(
                        """
                                value => {
                                    console.log("Autobench Single COre",Object.keys(value))
                                    if (value["workload"] != "autobench") {
                                        return null
                                    }
                                    if(!value["results"]) {
                                        return 0
                                    } else {\s
                                        return parseFloat(((value["results"].reduce((a,b) => a+b, 0))/value["results"].length).toFixed(3))
                                    }
                                };
                                """
                        )),
                new Label("Contexts",t)
                        .loadExtractors(
                                Extractor.fromString(transformPrefix+"$.contexts").setName("contexts")
                        ),
                new Label("CoreMark-PRO Multi Core",t)
                        .loadExtractors(
                                Extractor.fromString(transformPrefix+"$.results.\"CoreMark-PRO\".MultiCore").setName("coremark-pro-multi-core")
                        ),
                new Label("CoreMark-PRO Scaling",t)
                        .loadExtractors(
                                Extractor.fromString(transformPrefix+"$.results.\"CoreMark-PRO\".Scaling").setName("coremark-pro-scaling")
                        ),
                new Label("CoreMark-PRO Single Core",t)
                        .loadExtractors(
                                Extractor.fromString(transformPrefix+"$.results.\"CoreMark-PRO\".SingleCore").setName("coremark-pro-single-core")
                        ),
                new Label("Description",t)
                        .loadExtractors(
                                Extractor.fromString(transformPrefix+"$.metadata.description").setName("Description")
                        ),
                new Label("Hostname",t)
                        .loadExtractors(
                                Extractor.fromString(transformPrefix+"$.metadata.ansible_facts.env.HOSTNAME").setName("Hostname")
                        ),
                new Label("Kernel",t)
                        .loadExtractors(
                                Extractor.fromString(transformPrefix+"$.metadata.ansible_facts.kernel").setName("kernel")
                        ),
                new Label("Metadata",t)
                        .loadExtractors(
                                Extractor.fromString(transformPrefix+"$.metadata").setName("metadata")
                        ),
                new Label("PCP Time Series",t)
                        .loadExtractors(
                                Extractor.fromString(transformPrefix+"$.pcp_ts").setName("pcp_time_series")
                        ),
                new Label("Results",t)
                        .loadExtractors(
                                Extractor.fromString(transformPrefix+"$.results").setName("results")
                        ),
                new Label("RHIVOS Config",t)
                        .loadExtractors(
                                Extractor.fromString(transformPrefix+"$.metadata.rhivos_config").setName("RHIVOS Config")
                        ),
                new Label("Run ID",t)
                        .loadExtractors(
                                Extractor.fromString(transformPrefix+"$.metadata.run_id").setName("run_id")
                        ),
                new Label("Sample UUID",t)
                        .loadExtractors(
                                Extractor.fromString(transformPrefix+"$.sample_uuid").setName("sample_uuid")
                        ),
                new Label("Start Time",t)
                        .loadExtractors(
                                Extractor.fromString(transformPrefix+"$.metadata.start_time").setName("start_time")
                        ),
                new Label("Stressor",t)
                        .loadExtractors(
                                Extractor.fromString(transformPrefix+"$.stressor").setName("stressor")
                        ),
                new Label("User",t)
                        .loadExtractors(
                                Extractor.fromString(transformPrefix+"$.metadata.user").setName("user")
                        ),
                new Label("UUID",t)
                        .loadExtractors(
                                Extractor.fromString(transformPrefix+"$.metadata.uuid").setName("uuid")
                        ),
                new Label("Workers",t)
                        .loadExtractors(
                                Extractor.fromString(transformPrefix+"$.workers").setName("workers")
                        ),
                new Label("Workload",t)
                        .loadExtractors(
                                Extractor.fromString(transformPrefix+"$.workload").setName("workload")
                        )
        );
        t.persist();
        return t;
    }

    @GET
    @Path("{id}/labelValues")
    public List<LabelService.ValueMap> labelValues(
            @PathParam("id") int testId,
            @QueryParam("group") String group,
            @QueryParam("filter") @DefaultValue("{}") String filter,
            @QueryParam("before") @DefaultValue("") String before,
            @QueryParam("after") @DefaultValue("") String after,
            @QueryParam("filtering") @DefaultValue("true") boolean filtering,
            @QueryParam("metrics") @DefaultValue("true") boolean metrics,
            @QueryParam("sort") @DefaultValue("") String sort,
            @QueryParam("direction") @DefaultValue("Ascending") String direction,
            @QueryParam("limit") @DefaultValue(""+Integer.MAX_VALUE) int limit,
            @QueryParam("page") @DefaultValue("0") int page,
            @QueryParam("include") @Separator(",") List<String> include,
            @QueryParam("exclude") @Separator(",") List<String> exclude,
            @QueryParam("multiFilter") @DefaultValue("false") boolean multiFilter){
        if(group!=null && !group.isBlank()){
            //TODO call labelValues with schema
            return service.labelValues(group,testId,include,exclude);
        }else {
            return service.labelValues(testId, filter, before, after, sort, direction, limit, page, include, exclude, multiFilter);
        }
    }
}
