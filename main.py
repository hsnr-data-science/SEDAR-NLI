from dotenv import load_dotenv
from agent_graph.main_graph import MainGraph
from agent_graph.config import MainGraphConfig
from models.config import ModelConfig, Servers, Embeddings
from cache.cacheable import CacheableRegistry
from tools.custom_functions import register_methods, custom_function_config
from models.config import ModelConfig, Servers, Models
from states.agent_graph_state import get_initial_state

def setup(model_config: ModelConfig, human_confirmation: bool = False, prompt_compression: bool = False):
    register_methods()
    load_dotenv()
    CacheableRegistry.ensure_methods()

    embedding_model = ModelConfig(server=Servers.OLLAMA_HSNR, model=Embeddings.NOMIC_EMBED_TEXT, embedding_size=768)
    graph_config = MainGraphConfig(
        default_llm=model_config,
        embedding_model=embedding_model,
        full_doc_strings=False,
        prompt_compression=prompt_compression, # TODO: Compressing any JSON outputs in the prompt for the synthesizer or final_response also leads to wrong answers.
        human_confirmation=human_confirmation
    )

    main_graph = MainGraph(graph_config)
    custom_function_config.default_llm = model_config
    custom_function_config.embedding_model = embedding_model
    custom_function_config.prompt_compression = prompt_compression
    custom_function_config.human_confirmation = human_confirmation
    custom_function_config.full_doc_strings = False

    workflow = main_graph.compile_workflow()
    # main_graph.generate_graph_image(workflow)

    return workflow

if __name__ == "__main__":
    default_llm_config = ModelConfig(server=Servers.AZURE_OPENAI, model=Models.O4_MINI, temperature=0.1, reasoning_effort="high")
    workflow = setup(default_llm_config, human_confirmation=True, prompt_compression=False)

    # user_query = "Which ML experiments (notebooks) exist for the dataset 'Student_Scores'? Explain what is done in the first one."
    # user_query = "Get all versions of the dataset 'Usernames_4'"
    # user_query = "Delete the dataset 'Usernames_3'"
    # user_query = "Can you generate a semantic mapping of the Countries and Capitals datasets, ensuring the columns are labeled with the DBPedia ontology?"
    # user_query = "get all datasets"
    # user_query = "What is the weather like on Mars today?"

    user_query = "Create a semantic labeling for the Countries, Capitals and Currencies datasets. Use the DBPedia ontology for labeling. Convert it into a mapping and perform OBDA to find which capitals use which currencies."
    

    state = get_initial_state(user_query)
    state = workflow.invoke(state)
    print(state["messages"][-1].content)

    # from tools.tool_retrieval import ToolRetriever

    # tool_retriever = ToolRetriever(embedding_config=ModelConfig(server=Servers.OLLAMA_HSNR, model=Embeddings.NOMIC_EMBED_TEXT, embedding_size=768))
    # tool_retriever.rebuild_collection(CacheableRegistry.get_cacheable_classes())

