from sedarapi import SedarAPI
import json
import time

base_url = "http://localhost:5001"

sedar = SedarAPI(base_url)
sedar.login_gitlab()
default_workspace = sedar.get_default_workspace()
sedar.logout()
sedar.login("admin", "admin")

default_workspace = sedar.get_default_workspace()
users = default_workspace.get_workspace_users()

for user in users:
    if user.username == "nmitte":
        user.update(is_admin=True)

sedar.logout()
sedar.login_gitlab()

dbpedia_ontology = default_workspace.create_ontology("DBPedia", "", "./data/dbpedia_2016-10.ttl")

ontologies = default_workspace.get_all_ontologies()

dcat_ontology = None
dbpedia_ontology = None

for ontology in ontologies:
    if ontology.title == "DCAT3":
        dcat_ontology = ontology
    if ontology.title == "DBPedia":
        dbpedia_ontology = ontology

# print(dcat_ontology.content)
# print(dbpedia_ontology.content)

currency_annotation = default_workspace.ontology_annotation_search("Currency", dbpedia_ontology)[0]
chemistry_annotation = default_workspace.ontology_annotation_search("chemical substance", dbpedia_ontology)[0]
capital_annotation = default_workspace.ontology_annotation_search("Capital", dbpedia_ontology)[0]
country_annotation = [a for a in default_workspace.ontology_annotation_search("Country", dbpedia_ontology) if a.title == "country"][0]
dataset_annotation = [a for a in default_workspace.ontology_annotation_search("Dataset", dcat_ontology) if a.title == "dataset"][0]

country_datasource_definition = {
    "name": "Countries",
    "read_format":"csv",
    "read_options":
        {
            "delimiter":";",
            "header":"true",
            "inferSchema":"true"
        },
    "write_type":"DELTA",
    "read_type":"SOURCE_FILE",
    "id_column":"index",
    "source_files":["Country"]
}

currency_datasource_definition = {
    "name": "Currencies",
    "read_format":"csv",
    "read_options":
        {
            "delimiter":";",
            "header":"true",
            "inferSchema":"true"
        },
    "write_type":"DELTA",
    "read_type":"SOURCE_FILE",
    "id_column":"ID",
    "source_files":["Currency"]
}

capital_datasource_definition = {
   "name":"Capitals",
   "read_format":"json",
   "read_options":{
      "multiLine":"true"
   },
   "spark_packages":[
      "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0"
   ],
   "write_type":"DELTA",
   "read_type":"SOURCE_FILE",
   "read_format": "json",
   "read_options": {
        "multiLine": "true"
    },
   "id_column":"rowNumber",
   "source_files":[
      "Capitals"
   ]
}

student_scores_datasource_definition = {
    "name": "Student_Scores",
    "read_format":"csv",
    "read_options":
        {
            "delimiter":",",
            "header":"true",
            "inferSchema":"true"
        },
    "write_type":"DELTA",
    "read_type":"SOURCE_FILE",
    "id_column":"ID",
    "source_files":["student_scores"]
}

usernames_datasource_definition = {
    "name": "Usernames",
    "read_format":"csv",
    "read_options":
        {
            "delimiter":";",
            "header":"true",
            "inferSchema":"true"
        },
    "write_type":"DELTA",
    "read_type":"SOURCE_FILE",
    "id_column":"Identifier",
    "source_files":["username"]
}

usernames2_datasource_definition = {
    "name": "Usernames_2",
    "read_format":"csv",
    "read_options":
        {
            "delimiter":";",
            "header":"true",
            "inferSchema":"true"
        },
    "write_type":"DELTA",
    "read_type":"SOURCE_FILE",
    "id_column":"Identifier",
    "source_files":["sumting"]
}

usernames3_datasource_definition = {
    "name": "Usernames_3",
    "read_format":"csv",
    "read_options":
        {
            "delimiter":";",
            "header":"true",
            "inferSchema":"true"
        },
    "write_type":"DELTA",
    "read_type":"SOURCE_FILE",
    "id_column":"Identifier",
    "source_files":["sumting"]
}

usernames4_datasource_definition = {
    "name": "Usernames_4",
    "read_format":"csv",
    "read_options":
        {
            "delimiter":";",
            "header":"true",
            "inferSchema":"true"
        },
    "write_type":"DELTA",
    "read_type":"SOURCE_FILE",
    "id_column":"Identifier",
    "source_files":["username"]
}

usernames_4_update_v2_datasource_definition = {
    "name": "Usernames_4_update_v2",
    "read_format":"csv",
    "read_options":
        {
            "delimiter":";",
            "header":"true",
            "inferSchema":"true"
        },
    "write_type":"DELTA",
    "read_type":"SOURCE_FILE",
    "id_column":"Identifier",
    "source_files":["username_v2"]
}

usernames_4_update_v3_datasource_definition = {
    "name": "Usernames_4_update_v3",
    "read_format":"csv",
    "read_options":
        {
            "delimiter":";",
            "header":"true",
            "inferSchema":"true"
        },
    "write_type":"DELTA",
    "read_type":"SOURCE_FILE",
    "id_column":"Identifier",
    "source_files":["username_v3"]
}


chemistry_enzymes_datasource_definition = {
    "name": "Chemical_Experiment_Enzymes",
    "read_format":"csv",
    "read_options":
        {
            "delimiter":",",
            "header":"true",
            "inferSchema":"true"
        },
    "write_type":"DELTA",
    "read_type":"SOURCE_FILE",
    "id_column":"Enzyme_ID",
    "source_files":["chemical_experiment_enzymes"]
}

chemistry_materials_datasource_definition = {
    "name": "Chemical_Experiment_Materials",
    "read_format":"csv",
    "read_options":
        {
            "delimiter":",",
            "header":"true",
            "inferSchema":"true"
        },
    "write_type":"DELTA",
    "read_type":"SOURCE_FILE",
    "id_column":"Sample_ID",
    "source_files":["chemical_experiment_material_science"]
}

chemistry_organic_datasource_definition = {
    "name": "Chemical_Experiment_Organic_Compounds",
    "read_format":"csv",
    "read_options":
        {
            "delimiter":",",
            "header":"true",
            "inferSchema":"true"
        },
    "write_type":"DELTA",
    "read_type":"SOURCE_FILE",
    "id_column":"Compound_ID",
    "source_files":["chemical_experiment_organic_properties"]
}

chemistry_reaction_datasource_definition = {
    "name": "Chemical_Experiment_Reaction_Kinetics",
    "read_format":"csv",
    "read_options":
        {
            "delimiter":",",
            "header":"true",
            "inferSchema":"true"
        },
    "write_type":"DELTA",
    "read_type":"SOURCE_FILE",
    "id_column":"Experiment_ID",
    "source_files":["chemical_experiment_reaction_kinetics"]
}

sensor_data_datasource_definition = {
    "name": "Sensor_Data",
    "read_format":"csv",
    "read_options":
        {
            "delimiter":",",
            "header":"true",
            "inferSchema":"true"
        },
    "write_type":"DELTA",
    "read_type":"SOURCE_FILE",
    "id_column":"ID",
    "source_files":["sensor_data"]
}


country_dataset = default_workspace.create_dataset(country_datasource_definition, "./data/Country.csv")
country_dataset.ingest()
country_dataset.add_tag(dbpedia_ontology, country_annotation)
country_dataset.publish()
country_id = country_dataset.id

currency_dataset = default_workspace.create_dataset(currency_datasource_definition, "./data/Currency.csv")
currency_dataset.ingest()
currency_dataset.add_tag(dbpedia_ontology, currency_annotation)
currency_dataset.publish()
currency_id = currency_dataset.id

capital_dataset = default_workspace.create_dataset(capital_datasource_definition, "./data/Capitals.json")
capital_dataset.ingest()
capital_dataset.add_tag(dbpedia_ontology, capital_annotation)
capital_dataset.publish()
capital_id = capital_dataset.id

student_scores_dataset = default_workspace.create_dataset(student_scores_datasource_definition, "./data/student_scores.csv")
student_scores_dataset.ingest()
student_scores_dataset.add_tag(dcat_ontology, dataset_annotation)
student_scores_dataset.publish()

dataset = default_workspace.create_dataset(usernames_datasource_definition, "./data/username.csv")
dataset.ingest()
dataset.add_tag(dcat_ontology, dataset_annotation)
dataset.publish()

dataset = default_workspace.create_dataset(usernames2_datasource_definition, "./data/sumting.csv")
dataset.ingest()
dataset.add_tag(dcat_ontology, dataset_annotation)
dataset.publish()

dataset = default_workspace.create_dataset(usernames3_datasource_definition, "./data/sumting.csv")
dataset.ingest()
dataset.add_tag(dcat_ontology, dataset_annotation)
dataset.publish()

dataset = default_workspace.create_dataset(usernames4_datasource_definition, "./data/username.csv")
dataset.ingest()
dataset.add_tag(dcat_ontology, dataset_annotation)
dataset.publish()

dataset = default_workspace.create_dataset(chemistry_enzymes_datasource_definition, "./data/chemical_experiment_enzymes.csv")
dataset.ingest()
dataset.add_tag(dbpedia_ontology, chemistry_annotation)
dataset.publish()

dataset = default_workspace.create_dataset(chemistry_materials_datasource_definition, "./data/chemical_experiment_material_science.csv")
dataset.ingest()
dataset.add_tag(dbpedia_ontology, chemistry_annotation)
dataset.publish()

dataset = default_workspace.create_dataset(chemistry_organic_datasource_definition, "./data/chemical_experiment_organic_properties.csv")
dataset.ingest()
dataset.add_tag(dbpedia_ontology, chemistry_annotation)
dataset.publish()

dataset = default_workspace.create_dataset(chemistry_reaction_datasource_definition, "./data/chemical_experiment_reaction_kinetics.csv")
dataset.ingest()
dataset.add_tag(dbpedia_ontology, chemistry_annotation)
dataset.publish()

dataset.update(description="This dataset focuses on aqueous solutions and reaction kinetics.")

dataset = default_workspace.create_dataset(sensor_data_datasource_definition, "./data/sensor_data.csv")
dataset.ingest()
dataset.add_tag(dcat_ontology, dataset_annotation)
dataset.publish()

# Wait for the ingestions to complete
time.sleep(5*60)

# Create experiment and add 2 notebooks
first_experiment = default_workspace.create_experiment("First Experiment")
notebook = first_experiment.create_automl_run(
    library_name="AutoGluon",
    datasets=[student_scores_dataset],
    title="Regression Run 1",
    description="",
    target_column="Scores",
    data_type="tabular",
    task_type="regression",
    problem_type="regression",
    user_params={},
    is_public=False,
    include_llm_features=False,
    create_with_llm=False
)

notebook = first_experiment.create_automl_run(
    library_name="AutoGluon",
    datasets=[student_scores_dataset],
    title="Regression Run 2",
    description="",
    target_column="Scores",
    data_type="tabular",
    task_type="regression",
    problem_type="regression",
    user_params={},
    is_public=False,
    include_llm_features=False,
    create_with_llm=False
)

# Create joined dataset and create dataset with lineage
username_4_dataset = None
country_dataset = None
capital_dataset = None
currency_dataset = None
country_id = ""
capital_id = ""
currency_id = ""

for dataset in default_workspace.get_all_datasets():
    if dataset.title == "Countries":
        country_dataset = dataset
        country_id = dataset.id
    if dataset.title == "Capitals":
        capital_dataset = dataset
        capital_id = dataset.id
    if dataset.title == "Currencies":
        currency_dataset = dataset
        currency_id = dataset.id
    if dataset.title == "Usernames_4":
        username_4_dataset = dataset

country_attribute_id = ""
attributes = country_dataset.get_all_attributes()
for attribute in attributes:
    if attribute.name == "Country":
        country_attribute_id = attribute.id

currency_attribute_id = ""
attributes = currency_dataset.get_all_attributes()
for attribute in attributes:
    if attribute.name == "ENTITY":
        currency_attribute_id = attribute.id

capital_attribute_id = ""
attributes = capital_dataset.get_all_attributes()
for attribute in attributes:
    if attribute.name == "col1":
        capital_attribute_id = attribute.id

join_data = f'[{{"type":"export","x":923,"y":304,"name":"Join Test","target":"HDFS","isPolymorph":false,"setFk":false,"setPk":false,"auto":true,"write_type":"DEFAULT","input":[{{"type":"join","x":876,"y":308,"input":[{{"input":[{{"type":"join","x":548,"y":220,"input":[{{"input":[{{"type":"data_source","x":379,"y":209,"uid":"{country_id}"}}],"column":"Country","columnID":"{country_attribute_id}","isJoinInput":true}},{{"input":[{{"type":"data_source","x":317,"y":276,"uid":"{capital_id}"}}],"column":"col1","columnID":"{capital_attribute_id}","isJoinInput":true}}]}}],"column":"Country","columnID":"{country_attribute_id}","isJoinInput":true}},{{"input":[{{"type":"data_source","x":611,"y":393,"uid":"{currency_id}"}}],"column":"ENTITY","columnID":"{currency_attribute_id}","isJoinInput":true}}]}}]}}]'

headers = {"Content-Type": "application/json"}

response = sedar.connection.session.post(
    f"{base_url}/api/v1/workspaces/{default_workspace.id}/workflow",
    data=json.dumps(json.loads(join_data)),
    headers=headers
)

# print(f"Status code: {response.status_code}")
# print(f"Response: {response.content}")

# Create updated versions of the Usernames_4 dataset
username_4_dataset.update_datasource(usernames_4_update_v2_datasource_definition, "./data/username_v2.csv")
time.sleep(60)
username_4_dataset.update_datasource(usernames_4_update_v3_datasource_definition, "./data/username_v3.csv")

# Add semantic mapping (Countries, Capitals, Curencies)
mapping_file = f"""
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix rml: <http://semweb.mmlab.be/ns/rml#> .
@prefix nosql: <http://purl.org/db/nosql#> .
@prefix dbo: <http://dbpedia.org/ontology/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

<#1> a rr:TriplesMap;
    rml:logicalSource [
        rml:source "{country_id}";
        nosql:store nosql:csv
    ];
    rr:subjectMap [
        rr:template "index";
        rr:class <http://dbpedia.org/ontology/Country>
    ];


    rr:predicateObjectMap [
        rr:predicate <http://dbpedia.org/ontology/GovernmentType>;
        rr:objectMap [rml:reference "Government"]
    ];


    rr:predicateObjectMap [
        rr:predicate <http://dbpedia.org/ontology/Country>;
        rr:objectMap [rml:reference "Country"]
    ];


    rr:predicateObjectMap [
        rr:predicate <http://dbpedia.org/ontology/Population>;
        rr:objectMap [rml:reference "Population"]
    ].



<#2> a rr:TriplesMap;
    rml:logicalSource [
        rml:source "{currency_id}";
        nosql:store nosql:csv
    ];
    rr:subjectMap [
        rr:template "ID";
        rr:class <http://dbpedia.org/ontology/Currency>
    ];


    rr:predicateObjectMap [
        rr:predicate <http://dbpedia.org/ontology/Currency>;
        rr:objectMap [rml:reference "CURRENCY"]
    ];


    rr:predicateObjectMap [
        rr:predicate <http://dbpedia.org/ontology/Country>;
        rr:objectMap [rml:reference "ENTITY"]
    ].



<#3> a rr:TriplesMap;
    rml:logicalSource [
        rml:source "{capital_id}";
        nosql:store nosql:json
    ];
    rr:subjectMap [
        rr:template "rowNumber";
        rr:class <http://dbpedia.org/ontology/Capital>
    ];


    rr:predicateObjectMap [
        rr:predicate <http://dbpedia.org/ontology/Country>;
        rr:objectMap [rml:reference "col1"]
    ];


    rr:predicateObjectMap [
        rr:predicate <http://dbpedia.org/ontology/City>;
        rr:objectMap [rml:reference "col2"]
    ].
"""

default_workspace.create_semantic_mapping("Countries_Currencies_Capitals", "Semantic Mapping for Countries, Currencies and Capitals", mapping_file)

datasets = default_workspace.get_all_datasets()

for dataset in datasets:
    # print(dataset.content)
    if dataset.title == "Join Test":
        dataset.ingest()
        dataset.publish()