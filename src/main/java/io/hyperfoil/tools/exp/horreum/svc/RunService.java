package io.hyperfoil.tools.exp.horreum.svc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.hyperfoil.tools.exp.horreum.entity.Run;
import io.hyperfoil.tools.exp.horreum.entity.Test;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.hibernate.service.spi.ServiceException;
import org.jboss.resteasy.reactive.RestForm;
import org.jboss.resteasy.reactive.multipart.FileUpload;

import javax.print.attribute.standard.Media;

@Path("/api/run")
@Produces(MediaType.APPLICATION_JSON)
public class RunService {


    @Inject
    TestService testService;

    @Inject
    LabelService labelService;

    @POST
    @Path("data")
    public Response addRunFromData(
            @QueryParam("test") String test,
            String content
    ){
        //String test = "rhivos-perf-comprehensive";
        try {
            JsonNode json = new ObjectMapper().readValue(content, JsonNode.class);
            Run r = createRun(test,json,new ObjectMapper().getNodeFactory().objectNode());
            labelService.calculateLabelValues(r.test.labels, r.id);
            return Response.ok().entity(r.id).build();
        } catch (JsonProcessingException e) {
            return Response.status(500).entity(e.getMessage()).build();
        }
    }

    @Transactional
    Run createRun(String test,JsonNode json,JsonNode metadata){
        Test t = testService.getByName(test);
        if(t==null){
            return null;
        }
        Run r = new Run(t.id,json,metadata);
        r.persistAndFlush();
        return r;
    }
}
