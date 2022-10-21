#!/usr/bin/env bash

TOPAZ_EDGE_DS_SVC=localhost:9292
SESSION_ID=b3062a3a-3a4e-42f1-ad01-43975c639f9e
 
objects() {
    cat tests.json | jq -c '.objects[]'    
}

relations() {
    cat tests.json | jq -c '.relations[]'    
}

while IFS='' read -r data; do

read -r -d '' DATA <<-EOM
{
    "object": ${data}
}
EOM
    echo ${DATA} | jq .

    grpcurl \
    -H "aserto-session-id: ${SESSION_ID}" \
    -plaintext \
    -d "${DATA}" \
    ${TOPAZ_EDGE_DS_SVC} \
    aserto.directory.writer.v2.Writer.SetObject | jq '.'

done < <(objects)

while IFS='' read -r data; do

read -r -d '' DATA <<-EOM
{
    "relation": ${data}
}
EOM
    echo ${DATA} | jq .

    grpcurl \
    -H "aserto-session-id: ${SESSION_ID}" \
    -plaintext \
    -d "${DATA}" \
    ${TOPAZ_EDGE_DS_SVC} \
    aserto.directory.writer.v2.Writer.SetRelation | jq '.'

done < <(relations)
