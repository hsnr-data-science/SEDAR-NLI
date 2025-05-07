from sedarapi import SedarAPI
from sedarapi.semantic_model import SemanticModel

base_url = "http://localhost:5001"

sedar = SedarAPI(base_url)
sedar.login_gitlab()
# print(sedar.connection.jupyter_token)
# sedar.login("admin", "admin")

default_workspace = sedar.get_default_workspace()

# usernames3_datasource_definition = {
#     "name": "Usernames_3",
#     "read_format":"csv",
#     "read_options":
#         {
#             "delimiter":";",
#             "header":"true",
#             "inferSchema":"true"
#         },
#     "write_type":"DELTA",
#     "read_type":"SOURCE_FILE",
#     "id_column":"Identifier",
#     "source_files":["sumting"]
# }

# dataset = default_workspace.create_dataset(usernames3_datasource_definition, "./data/sumting.csv")
# dataset.ingest()
# dataset.publish()

# usernames_additional_infos = None

# for dataset in default_workspace.get_all_datasets():
#     if dataset.title == "Usernames_Additional_Infos":
#         usernames_additional_infos = dataset


# usernames_additional_infos.update("Usernames_2")

test123 = None

for dataset in default_workspace.get_all_datasets():
    if dataset.title == "Test123":
        test123 = dataset

test123.publish()

print()