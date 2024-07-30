package io.hyperfoil.tools.exp.horreum.svc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.hyperfoil.tools.exp.horreum.entity.*;
import io.hyperfoil.tools.exp.horreum.entity.extractor.Extractor;
import io.hyperfoil.tools.exp.horreum.pasted.JsonBinaryType;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.*;
import org.hibernate.query.NativeQuery;

import java.util.*;

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/api/labels")
@Produces(APPLICATION_JSON)
public class LabelService {

    //adding testId is likely unnecessary but those dashboard requests could lead to cross-test comparisons
    public static record ValueMap(ObjectNode data,long index,long labelId,long runId,long testId){}

    //cannot be transactional and cannot be run in a transaction as it must write each to db as it runs
    public void calculateLabelValues(Collection<Label> labels, Long runId){
        for(Label l : labels){
            calculateLabelValue(l,runId);
        }
    }

    @Transactional
    public void calculateLabelValue(Label l, Long runId){

        ExtractedValues extractedValues = calculateExtractedValuesWithIterated(l,runId);
        Run r = Run.findById(runId);
        boolean isIterated = usesIterated(runId,l.id);
        System.out.println("calculateLabelValue "+l.name+" isIterated = "+isIterated);
        if(isIterated){
            if(l.extractors.size()==1){
                //we do not have to deal with multitype
                List<ExtractedValue> evs = extractedValues.get(l.extractors.get(0).name);
                for(ExtractedValue ev : evs){
                    if(ev.isIterated){ //I think this should always be true
                        if(ev.data.isArray()){
                            ArrayNode arrayNode = (ArrayNode) ev.data;
                            for(JsonNode childNode : arrayNode){
                                LabelValue newValue = new LabelValue();
                                newValue.data = childNode;
                                if(l.reducer!=null){
                                    newValue.data = l.reducer.evalJavascript(newValue.data);
                                }
                                newValue.label = l;
                                newValue.run = r;
                                if(ev.sourceValueId() > 0) {
                                    LabelValue referenced = LabelValue.findById(ev.sourceValueId);
                                    newValue.sources.add(referenced);
                                }else{
                                    //it doesn't have a source, this is ok
                                }
                                System.out.println("persisting iterated "+l.name+" data="+newValue.data);
                                newValue.persist();
                            }
                        }else{
                            //this means an error occurred in calculating
                        }
                    }else{
                        //
                        LabelValue newValue = new LabelValue();
                        newValue.data = ev.data;
                        if(l.reducer!=null){
                            newValue.data = l.reducer.evalJavascript(newValue.data);
                        }
                        newValue.label = l;
                        newValue.run = r;
                        newValue.persist();
                        if(ev.sourceValueId > 0){
                            //this can happen when the value is derived from a previous label but not iterated
                            LabelValue referenced = LabelValue.findById(ev.sourceValueId);
                            newValue.sources.add(referenced);
                        }
                        System.out.println("persisting NOT-iterated "+l.name+" data="+newValue.data);
                        newValue.persist();
                    }
                }
            } else {
                //this is the challenging bit where we have to deal with NxN et.al.
                //TODO write this bit
                System.out.println("need to figure out iterated multi extractor for "+l.name);
            }
        } else {
            //not iterated,
            if(l.extractors.size()==1){
                List<ExtractedValue> evs = extractedValues.get(l.extractors.get(0).name);
                for(ExtractedValue ev : evs){
                    if(ev.isIterated){
                        //whoops, how did this happen
                        System.out.println("HOW DID THIS HAPPEN? "+l.name+" is not iterated but extracted value is\n"+ev);
                    }
                    LabelValue newValue = new LabelValue();
                    newValue.data = ev.data;
                    if(l.reducer!=null){
                        newValue.data = l.reducer.evalJavascript(newValue.data);
                    }
                    newValue.label = l;
                    newValue.run = r;
                    if(ev.sourceValueId > 0){
                        //this can happen when the value is derived from a previous label but not iterated
                        LabelValue referenced = LabelValue.findById(ev.sourceValueId);
                        newValue.sources.add(referenced);
                    }
                    newValue.persist();
                }
            }else{
                System.out.println("we need to figure out multi extractor non iterated for "+l.name);
            }

        }

    }

    public record ExtractedValue(long sourceValueId, boolean isIterated, JsonNode data){}

    public static class ExtractedValues {
        private Map<String,List<ExtractedValue>> values = new HashMap();

        public void add(String name,long sourceId,boolean iterated,JsonNode data){
            if(data == null || data.isNull()){
                return;
            }
            if (!values.containsKey(name)) {
                values.put(name,new ArrayList<>());
            }
            ExtractedValue v = new ExtractedValue(sourceId,iterated,data);
            values.get(name).add(v);
        }
        public boolean hasNonNull(String name){
            return values.containsKey(name) && !values.get(name).isEmpty();
        }
        public List<ExtractedValue> get(String name){
            return values.get(name);
        }
        public int size(){return values.size();}
        public Set<String> getNames(){return values.keySet();}
        public ObjectNode asNode(){
            ObjectNode rtrn = JsonNodeFactory.instance.objectNode();
            for(String name : getNames()){
                List<ExtractedValue> extractedValues = get(name);
                if(extractedValues.size()==0){
                    rtrn.set(name,JsonNodeFactory.instance.nullNode());
                }else if(extractedValues.size()==1){
                    rtrn.set(name,extractedValues.get(0).data);
                }else{
                    ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
                    for(ExtractedValue v : extractedValues){
                        arrayNode.add(v.data);
                    }
                    rtrn.set(name,arrayNode);
                }
            }
            return rtrn;
        }
        public String toString(){
            StringBuilder sb = new StringBuilder();
            for(String name : getNames()){
                sb.append("\n  "+name+" "+get(name).size());
                for( ExtractedValue v : get(name)){
                    sb.append("\n    "+v.sourceValueId+" "+v.isIterated+" "+v.data);
                }
            }
            return sb.toString();
        }
    }
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

    /*
     * Get all the LabelValues for the given test that target the specific schema.
     */
    public List<LabelValue> getBySchema(String schema,Long testId){
        return LabelValue.find("from LabelValue lv where lv.label.target_schema = ?1 and lv.label.parent.id = ?2",schema,testId).list();
    }


    public List<Label> getDescendantLabels(Long labelId){
        List<Label> rtrn = new ArrayList<>();

        //noinspection unchecked
        Label.getEntityManager().createNativeQuery(
            """
                with recursive bag(id) as ( values(:labelId) union select l.id from bag b,extractor e left join label l on e.parent_id = l.id where e.target_id =b.id) select * from bag
                """
        ).setParameter("labelId",labelId)
        .unwrap(NativeQuery.class)
        .addScalar("id",Long.class)
        .list()
                .forEach(v->{
                    Long id = (Long)v;
                    Label found = Label.getEntityManager().getReference(Label.class,id);
                    rtrn.add(found);
                });
        return rtrn;
    }

    /*
     * Gets the LabelValues that reference the given index in parent labelValue
     */
    public List<LabelValue> getDerivedValues(LabelValue parent,int index){
        return LabelValue.find("from LabelValue lv where exists (from LabelValuePointer lvp where lvp.child = lv and lvp.target=?1 and lvp.targetIndex = ?2)",parent,index).list();
    }

    //get the labelValues for all instances of a target schema for a test
    //could also have a labelValues based on label name, would that be useful? label name would not be merge-able across multiple labels
    public List<ValueMap> labelValues(String schema,long testId, List<String> include, List<String> exclude){
        List<ValueMap> rtrn = new ArrayList<>();
        String labelNameFilter = "";
        if (include!=null && !include.isEmpty()){
            if(exclude!=null && !exclude.isEmpty()){
                include = new ArrayList<>(include);
                include.removeAll(exclude);
            }
            if(!include.isEmpty()) {
                labelNameFilter = " AND l.name in :include";
            }
        }
        //includeExcludeSql is empty if include did not contain entries after exclude removal
        if(labelNameFilter.isEmpty() && exclude!=null && !exclude.isEmpty()){
            labelNameFilter=" AND l.name NOT in :exclude";
        }

        //noinspection rawtypes
        NativeQuery query = (NativeQuery) em.createNativeQuery(
        """
        with bag as (
            select
                r.test_id, lv.run_id, lt.id as target_label_id, lvp.targetindex, l.name,
                jsonb_agg(lv.data -> lvp.childindex::int) as data
            from label_values lv
                right join label_value_pointer lvp on lvp.child_id = lv.id
                left join label l on l.id = lv.label_id
                left join label_values lvt on lvp.target_id = lvt.id
                left join label lt on lt.id = lvt.label_id
                left join run r on r.id = lv.run_id
            where lt.target_schema = :schema and r.test_id = :testId
            LABEL_NAME_FILTER
            group by r.test_id,lv.run_id,lt.id,lvp.targetindex, l.name
        )
        select
            targetindex,target_label_id, run_id, test_id,
            jsonb_object_agg(name,(case when jsonb_array_length(data) > 1 then data else data->0 end)) as data
        from bag
        group by test_id,run_id,target_label_id,targetindex;
        """.replace("LABEL_NAME_FILTER",labelNameFilter)
        ).setParameter("schema",schema)
        .setParameter("testId",testId);

        if(!labelNameFilter.isEmpty()){
            if(labelNameFilter.contains("include")){
                query.setParameter("include",include);
            }
            if(labelNameFilter.contains("exclude")){
                query.setParameter("exclude",exclude);
            }
        }

        //noinspection unchecked
        List<Object[]> found = query
            .unwrap(NativeQuery.class)
            .addScalar("targetindex",Long.class)
            .addScalar("target_label_id",Long.class)
            .addScalar("run_id",Long.class)
            .addScalar("test_id",Long.class)
            .addScalar("data",JsonBinaryType.INSTANCE)
            .list();

        for(Object[] object : found){
            // tuple (labelId,index) should uniquely identify which label_value entry "owns" the ValueMap for the given test and run
            // note a label_value can have multiple values that are associated with a (labelId,index) if it is NxN
            Long index = (Long)object[0];
            Long labelId = (Long)object[1];
            Long runId = (Long)object[2];
            //object[3] is testId
            ObjectNode data = (ObjectNode)object[4];

            ValueMap vm = new ValueMap(data,index,labelId,runId,testId);
            rtrn.add(vm);
        }
        return rtrn;
    }
    //This is the labelValues endpoint that more closely matches what currently exists in Horreum if run = dataset
    //filter,before,after,sort,direction,limit,page, and multiFilter are not yet supported
    List<LabelService.ValueMap> labelValues(
            long  testId,
            String filter,
            String before,
            String after,
            String sort,
            String direction,
            int limit,
            int page,
            List<String> include,
            List<String> exclude,
            boolean multiFilter){
        List<ValueMap> rtrn = new ArrayList<>();
        String labelNameFilter = "";
        if (include!=null && !include.isEmpty()){
            if(exclude!=null && !exclude.isEmpty()){
                include = new ArrayList<>(include);
                include.removeAll(exclude);
            }
            if(!include.isEmpty()) {
                labelNameFilter = " AND l.name in :include";
            }
        }
        //includeExcludeSql is empty if include did not contain entries after exclude removal
        if(labelNameFilter.isEmpty() && exclude!=null && !exclude.isEmpty()){
            labelNameFilter=" AND l.name NOT in :exclude";
        }

        //noinspection rawtypes
        NativeQuery query = (NativeQuery) em.createNativeQuery(
                        """
                        with bag as (
                            select
                                r.test_id, lv.run_id, l.name,
                                jsonb_agg(lv.data) as data
                            from label_values lv
                                left join label l on l.id = lv.label_id
                                left join run r on r.id = lv.run_id
                            where r.test_id = :testId
                            LABEL_NAME_FILTER
                            group by r.test_id,lv.run_id,l.name
                        )
                        select
                            run_id, test_id,
                            jsonb_object_agg(name,(case when jsonb_array_length(data) > 1 then data else data->0 end)) as data
                        from bag
                        group by test_id,run_id;
                        """.replace("LABEL_NAME_FILTER",labelNameFilter)
                )
                .setParameter("testId",testId);
        if(!labelNameFilter.isEmpty()){
            if(labelNameFilter.contains("include")){
                query.setParameter("include",include);
            }
            if(labelNameFilter.contains("exclude")){
                query.setParameter("exclude",exclude);
            }
        }

        //noinspection unchecked
        List<Object[]> found = query
                .unwrap(NativeQuery.class)
                .addScalar("run_id",Long.class)
                .addScalar("test_id",Long.class)
                .addScalar("data",JsonBinaryType.INSTANCE)
                .list();
        for(Object[] object : found){
            // tuple (labelId,index) should uniquely identify which label_value entry "owns" the ValueMap for the given test and run
            // note a label_value can have multiple values that are associated with a (labelId,index) if it is NxN
            Long runId = (Long)object[0];
            //object[1] is testId
            ObjectNode data = (ObjectNode)object[2];

            ValueMap vm = new ValueMap(data,-1,-1,runId,testId);
            rtrn.add(vm);
        }
        return rtrn;
    }

    public List<ValueMap> labelValues(long labelId, long runId, long testId){
        return labelValues(labelId,runId,testId,Collections.emptyList(),Collections.emptyList());
    }
    //testId is only needed to create the ValueMap because labels are currently scoped to a test
    public List<ValueMap> labelValues(long labelId, long runId, long testId, List<String> include, List<String> exclude){
        List<ValueMap> rtrn = new ArrayList<>();
        String labelNameFilter = "";
        if (include!=null && !include.isEmpty()){
            if(exclude!=null && !exclude.isEmpty()){
                include = new ArrayList<>(include);
                include.removeAll(exclude);
            }
            if(!include.isEmpty()) {
                labelNameFilter = " AND l.name in :include";
            }
        }
        //includeExcludeSql is empty if include did not contain entries after exclude removal
        if(labelNameFilter.isEmpty() && exclude!=null && !exclude.isEmpty()){
            labelNameFilter=" AND l.name NOT in :exclude";
        }
        //could not be done in hql because of the json manipulation
        @SuppressWarnings("rawtypes")
        NativeQuery query = ((NativeQuery)em.createNativeQuery(
        """
           with bag as (
                select lvp.targetindex,
                    lt.id as target_label_id,
                    l.name,jsonb_agg(lv.data -> lvp.childindex::int) as data
                from label_values lv
                    right join label_value_pointer lvp on lvp.child_id = lv.id
                    left join label_values lvt on lvp.target_id = lvt.id
                    left join label l on l.id = lv.label_id
                    left join label lt on lt.id = lvt.label_id
                where lt.id = :label_id and lv.run_id = :run_id
                    LABEL_NAME_FILTER
                group by target_label_id,targetindex,l.name)
           select
                targetindex as index,
                target_label_id as label_id,
                jsonb_object_agg(name,(case when jsonb_array_length(data) > 1 then data else data->0 end)) as data
           from bag
           group by target_label_id,targetindex order by targetindex asc
           """.replace("LABEL_NAME_FILTER",labelNameFilter)
            //is the target_run_id = :run_id necessary? I think target_run_id === child_run_id
        )
            .setParameter("label_id",labelId)
            .setParameter("run_id",runId));

        if(labelNameFilter.contains(":include")){
            query.setParameter("include",include);
        }else if (labelNameFilter.contains(":exclude")){
            query.setParameter("exclude",exclude);
        }

        query.unwrap(NativeQuery.class)
            .addScalar("index",Long.class)
            .addScalar("label_id",Long.class)
            .addScalar("data",JsonBinaryType.INSTANCE);

        //noinspection unchecked
        List<Object[]> found = query.list();
        for(Object[] object : found){
            ValueMap vm = new ValueMap((ObjectNode) object[2],(Long)object[0],(Long)object[1],runId,testId);
            rtrn.add(vm);
        }
        return rtrn;
    }



    /*
     * Detects direct dependency on an iterated label_value
     * @return true iff the label depends on an iterated label_value for the specific run
     */
    public boolean usesIterated(long runId, long labelId){
        Boolean response = (Boolean) em.createNativeQuery("""
                select exists (select 1 from extractor e left join label_values lv on e.target_id = lv.label_id where e.parent_id = :labelId and  lv.run_id = :runId and lv.iterated)
                """).setParameter("labelId",labelId)
                .setParameter("runId",runId)
                .unwrap(NativeQuery.class)
                .getSingleResult();
        return response != null && response;
    }

    public LabelValue getLabelValue(long runId, long labelId){
        return LabelValue.find("from LabelValue lv where lv.run.id=?1 and lv.label.id=?2",runId,labelId).firstResult();
    }

    private void debug(String sql,Object...args){
        List<Object> found;
        NativeQuery q = Label.getEntityManager().createNativeQuery(sql).unwrap(NativeQuery.class);
        for(int i=0; i<args.length; i++){
            q.setParameter(i+1,args[i]);
        }
        found = q.getResultList();
        if(found!=null){
            found.forEach(row->{
                if(row == null){
                    //
                }else{
                    if(row instanceof Object[]){
                        System.out.printf("%s%n",Arrays.asList((Object[])row).toString());
                    }else {
                        System.out.printf("%s%n",row.toString());
                    }
                }
            });
        }
    }

    /*
        LabelValueExtractor on an iterated label_value will need to run N separate times because it will be forced to be an iterated value
     */
    public ExtractedValues calculateExtractedValuesWithIterated(Label l, long runId){
        ExtractedValues rtrn = new ExtractedValues();

        //debugging again
        //a for-each that isn't iterated...?
        //when m.dtype = 'LabelValueExtractor' and m.jsonpath is not null and m.jsonpath != '' and m.foreach and jsonb_typeof(m.lv_data) = 'array' then extract_path_array(m.lv_data,m.jsonpath::jsonpath)

        //do we need to check jsonb_typeof
        //right now this assumes we don't get garbage data... probably not a safe assumption
        //unchecked is how you know the code is great :)
        @SuppressWarnings("unchecked")
        List<Object[]> found = Label.getEntityManager().createNativeQuery("""
            with m as (
                select
                    e.name, e.type, e.jsonpath, e.foreach, e.column_name,
                    lv.id as label_id,lv.data as lv_data, lv.iterated as lv_iterated,
                    r.data as run_data, r.metadata as run_metadata
                from
                    extractor e full join label_values lv on e.target_id = lv.label_id,
                    run r where e.parent_id = :label_id and (lv.run_id = :run_id or lv.run_id is null) and r.id = :run_id),
            n as (select m.name, m.type, m.jsonpath, m.foreach, m.lv_iterated ,m.label_id, (case
                when m.type = 'PATH' and m.jsonpath is not null then jsonb_path_query_array(m.run_data,m.jsonpath::jsonpath)
                when m.type = 'METADATA' and m.jsonpath is not null and m.column_name = 'metadata' then jsonb_path_query_array(m.run_metadata,m.jsonpath::jsonpath)
                when m.type = 'VALUE' and m.jsonpath is not null and m.jsonpath != '' and m.lv_iterated then extract_path_array(m.lv_data,m.jsonpath::jsonpath)
                
                when m.type = 'VALUE' and m.jsonpath is not null and m.jsonpath != '' then jsonb_path_query_array(m.lv_data,m.jsonpath::jsonpath)
                when m.type = 'VALUE' and (m.jsonpath is null or m.jsonpath = '') then to_jsonb(ARRAY[m.lv_data])
                else '[]'::jsonb end) as found from m)
            select n.name as name,n.label_id,(case when jsonb_array_length(n.found) > 1 or strpos(n.jsonpath,'[*]') > 0 then n.found else n.found->0 end) as data, n.lv_iterated as lv_iterated from n
        """).setParameter("run_id",runId).setParameter("label_id",l.id)
                //TODO add logging in else '[]'
                .unwrap(NativeQuery.class)
                .addScalar("name",String.class)
                .addScalar("label_id",Long.class)
                .addScalar("data", JsonBinaryType.INSTANCE)
                .addScalar("lv_iterated",Boolean.class)
                .getResultList();


        if(found.isEmpty()){
            //TODO alert error or assume the data missed all the labels?
        }else {
            for(int i=0; i<found.size(); i++){
                Object[] row = (Object[])found.get(i);
                String name = (String)row[0];
                Long labelId = (Long) row[1];
                JsonNode data = (JsonNode) row[2];
                Boolean iterated = (Boolean)row[3];
                System.out.println(name+" id="+labelId+" iterated="+iterated+" data="+data);
                rtrn.add(name,labelId == null ? -1 : labelId,iterated != null && iterated,data);
            }
        }
        return rtrn;
    }

}
