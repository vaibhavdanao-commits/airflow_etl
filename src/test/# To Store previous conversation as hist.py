# To Store previous conversation as history 
print("Program Started")
import os
# Load Google API Key
from mykey import key
# Imported Python and Langchain Libraries
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.chat_history import InMemoryChatMessageHistory
from langchain_core.runnables import RunnableWithMessageHistory

os.environ["GOOGLE_API_KEY"] = key
# Created Gemini LLM Object
llm_obj = ChatGoogleGenerativeAI(model = "gemini-3.6-flash")

prompt = ChatPromptTemplate.from_messages([
    ("system", "You're a helpful AI Assistant."),
     MessagesPlaceholder("history"),
     ("human", "\n\nQuestion:\n{ques}")
     ])

store = {}

def get_history(session_id):
    return store.setdefault(session_id, InMemoryChatMessageHistory())

conversation =  RunnableWithMessageHistory(
    prompt | llm_obj,
    get_history,
    input_messages_key = "ques",
    history_messages_key = "history"
)
my_sessionid = "user1"
username = input("\033[91m It is a Customized ChatBot! I hope you are well, May I know your name Please: \033[0m")
print(f"\033[92mBot:\033[0m Hi! \033[94m{username}\033[0m, Please ask your questions. To stop, type 'exit' or 'quit'")
# While loop for infinite conversation
while True:
    userquestion = input(f"\033[94m{username}: \033[0m")
    if userquestion.lower() in ['exit', 'quit']:
        print("\033[92mBot:\033[0m GoodBye! ", f"\033[94m{username}\033[0m")
        break
    else:
        result = conversation.invoke({"ques":userquestion}, config = {"configurable":{"session_id":my_sessionid}})
        print(result.content)