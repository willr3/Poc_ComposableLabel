package io.hyperfoil.tools.exp.horreum.svc;

import io.hyperfoil.tools.exp.horreum.entity.Label;
import io.hyperfoil.tools.exp.horreum.entity.extractor.LabelValueExtractor;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;

import java.util.*;
import java.util.stream.Collectors;

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/api/labels")
@Produces(APPLICATION_JSON)
public class LabelService {

    @Inject
    EntityManager em;

    @POST
    @Transactional
    public long add(Label label){
        if(label.id!=null && label.id == -1){
            label.id = null;
        }
        em.persist(label);
        em.flush();
        return label.id;
    }

    @GET
    public Label get(long id){
        return Label.findById(id);
    }




}
