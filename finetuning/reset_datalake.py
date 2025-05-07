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

dbpedia_ontology = default_workspace.create_ontology("DBPedia", "", "./finetuning/data/dbpedia_2016-10.ttl")

ontologies = default_workspace.get_all_ontologies()

dcat_ontology = None
dbpedia_ontology = None

for ontology in ontologies:
    if ontology.title == "DCAT3":
        dcat_ontology = ontology
    if ontology.title == "DBPedia":
        dbpedia_ontology = ontology

print(dcat_ontology.content)
print(dbpedia_ontology.content)

university_annotation = [a for a in default_workspace.ontology_annotation_search("University", dbpedia_ontology) if a.title.lower() == "university"][0]
dataset_annotation = [a for a in default_workspace.ontology_annotation_search("Dataset", dcat_ontology) if a.title == "dataset"][0]
location_annotation = [a for a in default_workspace.ontology_annotation_search("Location", dbpedia_ontology) if a.title == "location"][0]
ranking_annotation = [a for a in default_workspace.ontology_annotation_search("Ranking", dbpedia_ontology) if a.title == "ranking"][0]

customers_datasource_definition = {
    "name": "Customers",
    "read_format":"csv",
    "read_options":
        {
            "delimiter":",",
            "header":"true",
            "inferSchema":"true"
        },
    "write_type":"DELTA",
    "read_type":"SOURCE_FILE",
    "id_column":"Index",
    "source_files":["customers-100"]
}

cars_datasource_definition = {
    "name": "Cars",
    "read_format":"csv",
    "read_options":
        {
            "delimiter":",",
            "header":"true",
            "inferSchema":"true"
        },
    "write_type":"DELTA",
    "read_type":"SOURCE_FILE",
    "id_column":"id",
    "source_files":["mtcars"]
}

physics_constants_datasource_definition = {
    "name": "Physics_Constants",
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
    "source_files":["physics_constants"]
}

physics_experiments_datasource_definition = {
    "name": "Physics_Experiments",
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
    "source_files":["physics_experiments"]
}

physics_particles_datasource_definition = {
    "name": "Physics_Particles",
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
    "source_files":["physics_particles"]
}

physics_theories_datasource_definition = {
    "name": "Physics_Theories",
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
    "source_files":["physics_theories"]
}

physics_units_datasource_definition = {
    "name": "Physics_Units",
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
    "source_files":["physics_units"]
}

physics_units_2_datasource_definition = {
    "name": "Physics_Units_2",
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
    "source_files":["physics_units_2"]
}

physics_units_2_update_v2_datasource_definition = {
    "name": "Physics_Units_2_update_v2",
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
    "source_files":["physics_units_2_v2"]
}

physics_units_2_update_v3_datasource_definition = {
    "name": "Physics_Units_2_update_v3",
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
    "source_files":["physics_units_2_v3"]
}

employees_datasource_definition = {
    "name": "Employees",
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
    "source_files":["sample_employee_data"]
}

sales_datasource_definition = {
    "name": "Sales_Data",
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
    "source_files":["sample_sales_data"]
}

university_details_datasource_definition = {
    "name": "University_Details",
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
    "source_files":["university_details"]
}

university_locations_datasource_definition = {
    "name": "University_Locations",
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
    "source_files":["university_locations"]
}

university_rankings_datasource_definition = {
    "name": "University_Rankings",
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
    "source_files":["university_ranking"]
}

weather_datasource_definition = {
    "name": "Weather_Data",
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
    "source_files":["weather"]
}

productivity_scores_datasource_definition = {
    "name": "Productivity_Scores",
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
    "source_files":["productivity_scores"]
}

dataset = default_workspace.create_dataset(customers_datasource_definition, "./finetuning/data/customers-100.csv")
dataset.ingest()
dataset.add_tag(dcat_ontology, dataset_annotation)
dataset.publish()

dataset = default_workspace.create_dataset(cars_datasource_definition, "./finetuning/data/mtcars.csv")
dataset.ingest()
dataset.add_tag(dcat_ontology, dataset_annotation)
dataset.publish()

dataset = default_workspace.create_dataset(physics_constants_datasource_definition, "./finetuning/data/physics_constants.csv")
dataset.ingest()
dataset.add_tag(dcat_ontology, dataset_annotation)
dataset.publish()

dataset = default_workspace.create_dataset(physics_experiments_datasource_definition, "./finetuning/data/physics_experiments.csv")
dataset.ingest()
dataset.add_tag(dcat_ontology, dataset_annotation)
dataset.publish()

dataset = default_workspace.create_dataset(physics_particles_datasource_definition, "./finetuning/data/physics_particles.csv")
dataset.ingest()
dataset.add_tag(dcat_ontology, dataset_annotation)
dataset.publish()

dataset = default_workspace.create_dataset(physics_theories_datasource_definition, "./finetuning/data/physics_theories.csv")
dataset.ingest()
dataset.add_tag(dcat_ontology, dataset_annotation)
dataset.publish()

dataset = default_workspace.create_dataset(physics_units_datasource_definition, "./finetuning/data/physics_units.csv")
dataset.ingest()
dataset.add_tag(dcat_ontology, dataset_annotation)
dataset.publish()

dataset = default_workspace.create_dataset(physics_units_2_datasource_definition, "./finetuning/data/physics_units_2.csv")
dataset.ingest()
dataset.add_tag(dcat_ontology, dataset_annotation)
dataset.publish()

dataset = default_workspace.create_dataset(employees_datasource_definition, "./finetuning/data/sample_employee_data.csv")
dataset.ingest()
dataset.add_tag(dcat_ontology, dataset_annotation)
dataset.publish()

dataset = default_workspace.create_dataset(sales_datasource_definition, "./finetuning/data/sample_sales_data.csv")
dataset.ingest()
dataset.add_tag(dcat_ontology, dataset_annotation)
dataset.publish()

university_dataset = default_workspace.create_dataset(university_details_datasource_definition, "./finetuning/data/university_details.csv")
university_dataset.ingest()
university_dataset.add_tag(dbpedia_ontology, university_annotation)
university_dataset.publish()

university_locations_dataset = default_workspace.create_dataset(university_locations_datasource_definition, "./finetuning/data/university_locations.csv")
university_locations_dataset.ingest()
university_locations_dataset.add_tag(dbpedia_ontology, location_annotation)
university_locations_dataset.publish()

university_rankings_dataset = default_workspace.create_dataset(university_rankings_datasource_definition, "./finetuning/data/university_ranking.csv")
university_rankings_dataset.ingest()
university_rankings_dataset.add_tag(dbpedia_ontology, ranking_annotation)
university_rankings_dataset.publish()

dataset = default_workspace.create_dataset(weather_datasource_definition, "./finetuning/data/weather.csv")
dataset.ingest()
dataset.add_tag(dcat_ontology, dataset_annotation)
dataset.publish()

productivity_scores_dataset = default_workspace.create_dataset(productivity_scores_datasource_definition, "./finetuning/data/productivity_scores.csv")
productivity_scores_dataset.ingest()
productivity_scores_dataset.add_tag(dcat_ontology, dataset_annotation)
productivity_scores_dataset.publish()

# Wait for the ingestions to complete
time.sleep(5*60)

sample_experiment  = default_workspace.create_experiment("Sample Experiment")
notebook = sample_experiment.create_automl_run(
    library_name="AutoGluon",
    datasets=[productivity_scores_dataset],
    title="Regression Run 1",
    description="",
    target_column="Productivity_Score",
    data_type="tabular",
    task_type="regression",
    problem_type="regression",
    user_params={},
    is_public=False,
    include_llm_features=False,
    create_with_llm=False
)

notebook = sample_experiment.create_automl_run(
    library_name="AutoGluon",
    datasets=[productivity_scores_dataset],
    title="Regression Run 2",
    description="",
    target_column="Productivity_Score",
    data_type="tabular",
    task_type="regression",
    problem_type="regression",
    user_params={},
    is_public=False,
    include_llm_features=False,
    create_with_llm=False
)

# Create joined dataset and create dataset with lineage
physics_units_2_dataset = None

for dataset in default_workspace.get_all_datasets():
    if dataset.title == "Physics_Units_2":
        physics_units_2_dataset = dataset
    if dataset.title == "University_Details":
        university_dataset = dataset
    if dataset.title == "University_Locations":
        university_locations_dataset = dataset
    if dataset.title == "University_Rankings":
        university_rankings_dataset = dataset

university_attribute_id = ""
attributes = university_dataset.get_all_attributes()
for attribute in attributes:
    if attribute.name == "University":
        university_attribute_id = attribute.id

university_locations_attribute_id = ""
attributes = university_locations_dataset.get_all_attributes()
for attribute in attributes:
    if attribute.name == "University":
        university_locations_attribute_id = attribute.id

university_rankings_attribute_id = ""
attributes = university_rankings_dataset.get_all_attributes()
for attribute in attributes:
    if attribute.name == "University":
        university_rankings_attribute_id = attribute.id

join_data = f'[{{"type":"export","x":923,"y":304,"name":"Join Test","target":"HDFS","isPolymorph":false,"setFk":false,"setPk":false,"auto":true,"write_type":"DEFAULT","input":[{{"type":"join","x":876,"y":308,"input":[{{"input":[{{"type":"join","x":548,"y":220,"input":[{{"input":[{{"type":"data_source","x":379,"y":209,"uid":"{university_dataset.id}"}}],"column":"University","columnID":"{university_attribute_id}","isJoinInput":true}},{{"input":[{{"type":"data_source","x":317,"y":276,"uid":"{university_locations_dataset.id}"}}],"column":"University","columnID":"{university_locations_attribute_id}","isJoinInput":true}}]}}],"column":"University","columnID":"{university_attribute_id}","isJoinInput":true}},{{"input":[{{"type":"data_source","x":611,"y":393,"uid":"{university_rankings_dataset.id}"}}],"column":"University","columnID":"{university_rankings_attribute_id}","isJoinInput":true}}]}}]}}]'

headers = {"Content-Type": "application/json"}

response = sedar.connection.session.post(
    f"{base_url}/api/v1/workspaces/{default_workspace.id}/workflow",
    data=json.dumps(json.loads(join_data)),
    headers=headers
)

print(f"Status code: {response.status_code}")
print(f"Response: {response.content}")

# Create updated versions of the Usernames_4 dataset
physics_units_2_dataset.update_datasource(physics_units_2_update_v2_datasource_definition, "./finetuning/data/physics_units_2_v2.csv")
time.sleep(60)
physics_units_2_dataset.update_datasource(physics_units_2_update_v3_datasource_definition, "./finetuning/data/physics_units_2_v3.csv")

datasets = default_workspace.get_all_datasets()

for dataset in datasets:
    # print(dataset.content)
    if dataset.title == "Join Test":
        dataset.ingest()
        dataset.publish()