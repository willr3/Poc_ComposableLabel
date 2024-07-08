package io.hyperfoil.tools.exp.horreum.svc;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import org.jboss.resteasy.reactive.Separator;

import java.util.ArrayList;
import java.util.List;

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/api/test")
@Produces(APPLICATION_JSON)public class TestService {

    @Inject
    LabelService service;


    @GET
    @Path("{id}/labelValues")
    List<LabelService.ValueMap> labelValues(
            @PathParam("id") int testId,
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
        return service.labelValues(testId,filter,before,after,sort,direction,limit,page,include,exclude,multiFilter);
    }
}
