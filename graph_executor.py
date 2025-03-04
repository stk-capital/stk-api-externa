# backend/graph_executor.py

# Importing dependencies
import json
import logging
import os
import re
import base64
from typing import Dict, Any, List

import dotenv
from bson import ObjectId
from langgraph.graph import MessageGraph, END, START
from langchain_core.messages import HumanMessage, AIMessage, FunctionMessage, SystemMessage
from langchain_core.tools import tool
from motor.motor_asyncio import AsyncIOMotorClient

# Importing language models
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage, AIMessage,FunctionMessage,SystemMessage
from langchain_groq import ChatGroq
from langchain_nvidia_ai_endpoints import ChatNVIDIA

# Configuring
dotenv.load_dotenv()
logger = logging.getLogger(__name__)
client = AsyncIOMotorClient(os.getenv("MONGO_DB_URL"))
##client= pymongo.MongoClient(os.getenv("MONGO_DB_URL"))
db = client.crewai_db

# Global variables
list_system_default_variables = ['type', 'content', 'source', 'route', 'id', 'content_type', 'name']

# Auxiliary functions
def extract_brace_arguments(text):
    # Regular expression to match {{key:value}} pairs
    # text = '''The screen is still black. It might be in sleep mode or powered off. Let's try pressing a key to wake it up.{{route:Desktop Hotkey}}{{keys:["space"]}}'''
    # print("text:"+text)
    pattern = r"\{\{(.*?)\}\}"
    matches = re.findall(pattern, str(text))

    # Dictionary to store extracted key-value pairs
    extracted_dict = {}

    for match in matches:
        try:
            # Split the match into key and value at the first colon
            key, value = match.split(':', 1)
            extracted_dict[key.strip()] = value.strip()
        except ValueError:
            # Handle cases where there is no colon in the match
            continue

    return extracted_dict

# Main functions
async def create_tool(tool_data: Dict[str, Any]):
    #teste from mongo specific id tool
    #id_example = "66b3694763f584021939cb93"
    #tool_data = db.tools.find_one({"_id": ObjectId(id_example)})

    # Set the API keys as environment variables
    for key, value in tool_data['config']['api_keys'].items():
        os.environ[key] = value

    """Create a tool based on the stored configuration."""
    if tool_data['type'] == 'langchain' or tool_data['type'] == 'crewai':

        # For pre-defined tools, we can import and instantiate them dynamically
        dependency_name = tool_data['dependencies']
        module_name = tool_data['module']
        import_statement = f"from {dependency_name} import {module_name}"

        # Execute the import statement
        exec(import_statement)
        tool_class = eval(module_name)

        # Initialize the tool with any provided arguments
        tool_instance = tool_class(**tool_data['config']['arguments'])

        def function_instance(input:List[HumanMessage]):

            relevant_arguments = {}
            for j,i in enumerate(input[-1]):
                if i[1] and i[0] not in list_system_default_variables:
                    relevant_arguments[i[0]] = i[1]
            

            try:
                function_output = tool_instance.run(**relevant_arguments)
            except:
                function_output = tool_instance.run(input[-1].content)

            if function_output.endswith('.png') and  function_output.startswith('/tmp/'):
                
                input.append(FunctionMessage(content=function_output,source=tool_data['name'],content_type='image',name=tool_data['name'].replace(" ","_")))
            else:

                input.append(FunctionMessage(content=function_output,source=tool_data['name'],content_type='text',name=tool_data['name'].replace(" ","_")))


            return input

    elif tool_data['type'] == 'custom':

        #input = [HumanMessage(content='whats 3 plus 4', id='49a65137-4c3e-40b4-9143-9e4fffa401a5'), HumanMessage(content='{"a":3,"b":4}', id='6ca01e68-41a4-4094-bbec-cb04e7a7d29a')]
        def function_instance(input:List[FunctionMessage]):
            # input = [HumanMessage(content='aaaaa',source='User')]
            custom_function_code = tool_data['config']['custom_function']
            exec(custom_function_code)
            function_name_match = re.search(r'def (\w+)\(', custom_function_code)
            # function_output=''

            if function_name_match:
                 function_name = function_name_match.group(1)
            else:
                raise ValueError("Function name not found in the custom function code")

            relevant_arguments = {}
            for j,i in enumerate(input[-1]):
                if i[1] and i[0] not in list_system_default_variables:
                    relevant_arguments[i[0]] = i[1]

            try:
                #relevant_arguments={'search_term': 'when is the next olympic event'}
                function_output = locals()[function_name] (**relevant_arguments)
            except:
                function_output = locals()[function_name] (input[-1].content)

            #check if function output is a png image file path
            if function_output.endswith('.png') and  function_output.startswith('/tmp/'):
                input.append(FunctionMessage(content=function_output,source=tool_data['name'],content_type='image',name=tool_data['name'].replace(" ","_")))
            else:

                input.append(FunctionMessage(content=function_output,source=tool_data['name'],content_type='text',name=tool_data['name'].replace(" ","_")))

            return input

    return function_instance


async def create_agent(agent_data: Dict[str, Any], graph_id: str = None):
    #create_agent(db.agents.find_one({"_id": ObjectId(id_example)}))
    #teste from mongo specific id tool
    #graph_id="66a928486b640b8ef0626504"
    #id_example = "66b0d0158a3d918c9e65973c"
    #agent_data = db.agents.find_one({"_id": ObjectId(id_example)})
    #graph_data = db.graphs.find_one({"_id": ObjectId(graph_id)})
    #llm_data = db.llms.find_one({"_id": ObjectId(agent_data['llm'])})
    llm_data = await db.llms.find_one({"_id": ObjectId(agent_data['llm'])})
    
    async def grab_description_by_target_id(_id,type):
        #_id = "66a928bf6b640b8ef0626505"
        #type = "tool"
        if type == "end":
            return "use {{route:END}} to end the flow when you believe you made you best effort to get to the final goal"
        elif type=="tool":
            result = await db.tools.find_one({"_id": ObjectId(_id)})
            return result["description"]
        elif type == "agent":
            result = await db.agents.find_one({"_id": ObjectId(_id)})
            return result["backstory"]

    if graph_id:
        # Bring graph data from MongoDB
        graph_data = await db.graphs.find_one({"_id": ObjectId(graph_id)})

        # Get all target nodes
        all_targets = [{"label":x['data']['label'],
                        "_id":x["_id"],
                        "type":x["data"]["type"],
                        "description":await grab_description_by_target_id(x["_id"], x["data"]["type"])} 
                       for x in graph_data['nodes'] 
                       if x['_id'] in [i['target'] for i in graph_data['edges'] if i['data']['edgeType'] == 'conditional' and ObjectId(i['source']) == agent_data['_id']]]
    #input = [FunctionMessage(content='/tmp/screenshot_20240731_191024.png', name='Screenshot', id='4a7ace2f-d6dd-4041-9d23-8fc9c46d1c96', content_type='image', source='Screenshot')]

    def format_state(input:List[FunctionMessage],goal:str,target_nodes:list=None):

        #target_nodes = all_targets
        #goal = agent_data["goal"]
        
        #FIXME: Este trecho está causando um problema no roteamento
            # O texto abaixo está forçando o agente a sempre retornar uma rota específica,
            # o que interfere com o fluxo normal do grafo. Removi temporariamente para
            # permitir que o agente escolha a rota corretamente com base na lógica do grafo
            # e o grafo funcionou conforme o esperado.
            # Precisamos revisar esta parte para garantir que o agente tenha liberdade
            # para escolher a rota apropriada, mas ainda siga as regras do grafo.
        if len(target_nodes) > 1 or any(node['type'] != 'end' for node in target_nodes):
            goal += '''
                    
                    You should never return an output without routing to your given nodes always {{route:given_target}}
                    
                    '''
        
        if target_nodes:
            
            if [i["label"] for i in target_nodes if i["type"]=="end"]:
                
                end_message = ''' 

                Whenever you believe you concluded your goal and none actions are required 
                you should end the flow by returning {{route:END}} and {{content:yout final response}} which is gonna be your response to the human user

                '''
                goal += end_message

            if [i["label"] for i in target_nodes if i["type"]=="tool"]:
            
                tool_message = f''' 
                
                ##Tools## 

                You will have these tools at your disposal: 
                {str([i["label"] for i in target_nodes if i["type"]=="tool"])} 

                and this is how each one of them work and how you can use them:
                
                '''+ "\n\n".join([f"{node['label']}\n{node['description']}" for node in target_nodes if node["type"]=="tool"])

                goal += tool_message
            
            
            #agent_message
            if [i["label"] for i in target_nodes if i["type"]=="agent" and i['label']!=agent_data['name']]:

                agent_message = f''' 
            
                ##Agents## 

                You can route to these agents: 
                {str([i["label"] for i in target_nodes if i["type"]=="agent" and i['label']!=agent_data['name']])} 

                and this is how each one of them are specialized at:
                
                '''+ "\n\n".join([f"{node['label']}\n{node['description']}" for node in target_nodes if node["type"]=="agent"])

                goal += agent_message
            
        state = input.copy()

        #give it the goal
        state.insert(0,SystemMessage(content=goal))

        with open('state.json', 'w') as f:
            json.dump(str(state), f)
        
        
        # state = [i for i in state[:-1] if i.dict().get('content_type') != 'image'] + [state[-1]]

        #Threat for image content
        for i in range(len(state)):
            if ('content_type', 'image') in [k for j,k in enumerate(state[i])]:
                
                #del all elements that are image and are not the last element so we decrease the amount of images in the state
                if i == len(state)-1:
                    image_url = state[i].content
                    #grab only the file name without the path and extension

                    with open(image_url, 'rb') as image_file:
                        image_data = base64.b64encode(image_file.read()).decode("utf-8")
                    
                    state[i] = HumanMessage(content=[{"type": "text", f"text": f'''Image{i+1}'''},
                                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_data}"}}])

        #solve for if model is "claude" based then none two sucessive message can be assistant messages or(AIMessages)
        if "claude" in llm_data['model']:
            for i in range(len(state)-1):
                kwargs = state[i].__dict__
                try:del kwargs['type']
                except:pass

                state[i] = HumanMessage(**kwargs)
        #also for claude the final message cant end with white space
        if "claude" in llm_data['model'] and state[-1].content :
            state[-1].content = state[-1].content.strip()
        
        with open('state.json', 'w') as f:
            json.dump(str(state), f)
        
        return state

    def function_instance(input: List[AIMessage]):
        """
        Executes the main logic of the agent.
        """
        def initialize_llm():
            temperature = agent_data['temperature']
            map_dict = {
                "GROQ": ChatGroq,
                "NIM": ChatNVIDIA,
                "OPENAI": ChatOpenAI,
                "ANTHROPIC": ChatAnthropic,
                "GOOGLE": ChatGoogleGenerativeAI
            }
            return map_dict[llm_data['baseURL']](
                model=llm_data['model'],
                api_key=llm_data['apiKey'],
                temperature=temperature
            )

        def log_state(prefix, state):
            with open('state.json', 'w') as f:
                json.dump(f"{prefix}:{str(state)}", f)

        def process_llm_output(function_output):
            variables_dict = extract_brace_arguments(function_output)
            handle_routing(variables_dict, function_output)
            ensure_content(variables_dict, function_output)
            variables_dict['source'] = agent_data['name']
            return variables_dict

        def handle_routing(variables_dict, function_output):
            
            if len(all_targets) > 1:
                if 'route' not in variables_dict:
                    variables_dict['route'] = agent_data['name']
                    variables_dict['content'] = add_routing_message(function_output, " I forgot to provide the routing of the next node, so the message go back to me. Im going to think about it and provide the routing in the next message")
                elif not is_valid_route(variables_dict.get('route')):
                    variables_dict['route'] = agent_data['name']
                    variables_dict['content'] = add_routing_message(function_output, "I forgot to provide a routing to a node available to me, so the message go back to me. Im going to think about it and provide the right routing in the next message")

        def is_valid_route(route):
            valid_routes = [i["label"] for i in all_targets]
            return route in valid_routes or (route == 'END' and 'End' in valid_routes)

        def add_routing_message(content, reason):
            return f"{content}\nI {reason} of the next node, so the message goes back to me. I'm going to think about it and provide the correct routing in the next message."

        def ensure_content(variables_dict, function_output):
            if 'content' not in variables_dict:
                variables_dict['content'] = function_output

        # Main execution flow
        llm = initialize_llm()
        state = format_state(input, goal=agent_data['goal'], target_nodes=all_targets)

        log_state("Input", state)
        function_output = llm.invoke(state).content
        log_state("Output", state)

        variables_dict = process_llm_output(function_output)
        input.append(AIMessage(**variables_dict))

        print("Input:" + str(input))  # DEBUG
        return input

    return function_instance

async def convert_to_langgraph(graph_data: Dict[str, Any]) -> MessageGraph:
    #convert_to_langgraph(db.graphs.find_one({"_id": ObjectId(id_example)}))
    # id_example = "66a928486b640b8ef0626504"
    # graph_data = db.graphs.find_one({"_id": ObjectId(id_example)})

    workflow = MessageGraph()
    conditional_sources=[]

    # Add nodes
    for node in graph_data['nodes']:
        #node =graph_data['nodes'][2]
        if node['data']['type'] == 'agent':
            agent_data = await db.agents.find_one({"_id": ObjectId(node['_id'])})
            agent = await create_agent(agent_data,graph_id=graph_data["_id"])
            workflow.add_node(node['data']['label'], agent)
        elif node['data']['type'] == 'tool':
            tool_data = await db.tools.find_one({"_id": ObjectId(node['_id'])})
            tool = await create_tool(tool_data)
            workflow.add_node(node['data']['label'], tool)

    # Add edges
    for edge in graph_data['edges']:
        #edge =graph_data['edges'][3]

        if 'start' in edge['source']:
            target_label = next(node['data']['label'] for node in graph_data['nodes'] if node['_id'] == edge['target'])
            workflow.set_entry_point(target_label)
        elif 'end' in edge['target']:
            source_label = next(node['data']['label'] for node in graph_data['nodes'] if node['_id'] == edge['source'])
            workflow.set_finish_point(source_label)
        else:

            target_label = next(node['data']['label'] for node in graph_data['nodes'] if node['_id'] == edge['target'])
            source_label = next(node['data']['label'] for node in graph_data['nodes'] if node['_id'] == edge['source'])

            if edge['data']['edgeType'] == 'deterministic':

                workflow.add_edge(source_label, target_label)

            elif edge['data']['edgeType'] == 'conditional':

                if source_label in conditional_sources:pass
                else:
                    conditional_sources.append(source_label)
                    # Add conditional edge
                    all_targets = [x['data']['label'] for x in graph_data['nodes'] if x['_id'] in [i['target'] for i in graph_data['edges'] if i['data']['edgeType']=='conditional' and i['source'] == edge['source']]]
                    #print(all_targets)
                    def router(input: List[AIMessage]):
                        return input[-1].route

                    # print(all_targets)

                    map_list={}
                    for i in all_targets:
                        if 'End' in i:
                            map_list['END']=END
                        elif 'Start' in i:
                            map_list['START']=START
                        else:
                            map_list[i]=i
                    if map_list:
                        map_list[source_label]=source_label
       
                    workflow.add_conditional_edges(source_label,router,map_list)

    return workflow.compile()

async def execute_graph(graph_data: Dict[str, Any], input_data: str):
    # Main execution function
    #input_data = '{{user_name:ruhany}} {{user_email:ruhcsadvasd}}'
    try:
        graph = await convert_to_langgraph(graph_data)

        variables_dict = extract_brace_arguments(input_data)
        relevant_arguments = variables_dict
        
        for step in graph.stream([HumanMessage(content=input_data,source='User',**relevant_arguments)]):


            # Convert the step data to a JSON-serializable format
            serializable_step = {}
            for key, value in step.items():
                if isinstance(value, list):
                    serializable_step[key] = [
                        {"type": type(msg).__name__, "content": msg.content}
                        for msg in value
                    ]
                else:
                    serializable_step[key] = str(value)

            yield json.dumps({"step": serializable_step})

        #logger.debug("Graph execution completed")
        yield json.dumps({"status": "completed"})

    except Exception as e:
        error_message = f"Error during graph execution: {str(e)}"
        logger.exception(error_message)
        yield json.dumps({"error": error_message})

