import os
from dotenv import load_dotenv
from langchain.chat_models import init_chat_model
from langchain_core.messages import HumanMessage, SystemMessage

load_dotenv()

# Access the API key securely
api_key = os.getenv("GOOGLE_API_KEY")
os.environ["GOOGLE_API_KEY"] = api_key

model = init_chat_model("gemini-2.0-flash", model_provider="google_genai")

user_msg = input("Enter the text to translate: ")

messages = [
    SystemMessage("Translate the following from English into Italian"),
    HumanMessage(user_msg),
]

response = model.invoke(messages)
print(response)
print('\n\n'+ response.content)