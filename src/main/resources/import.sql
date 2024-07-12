CREATE OR REPLACE FUNCTION extract_path_array(_arr jsonb, _path jsonpath)
  RETURNS jsonb
  LANGUAGE sql STABLE PARALLEL SAFE AS
'
    with bag as (SELECT jsonb_path_query_array(elem,_path) as data FROM jsonb_array_elements(_arr) elem)
    select jsonb_agg((case when jsonb_array_length(data) > 1 OR strpos(_path::text,''[*]'') > 0 then data else data->0 end)) from bag;
';;
