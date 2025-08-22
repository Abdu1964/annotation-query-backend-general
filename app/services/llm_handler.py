import os
from dotenv import load_dotenv
from app.services.llm_models import OpenAIModel, GeminiModel
from app.services.graph_handler import Graph_Summarizer
import logging
load_dotenv()

class LLMHandler:
    def __init__(self):
        model_type = os.getenv('LLM_MODEL')

        if model_type == 'openai':
            openai_api_key = os.getenv('OPENAI_API_KEY')
            if not openai_api_key:
                logging.error("Error not a valid openai api key")
                self.model = None
            else:
                self.model = OpenAIModel(openai_api_key)
        elif model_type == 'gemini':
            gemini_api_key = os.getenv('GEMINI_API_KEY')
            if not gemini_api_key:
                logging.error("Error not a valid gemini api key")
                self.model = None
            else:
                self.model = GeminiModel(gemini_api_key)
        else:
            logging.error("Error not a valid model type")
            self.model = None

    def generate_title(self, query):
        if self.model is None:
            return "Untitled"
        prompt = f'''From this query generate approperiate title. Only give the title sentence don't add any prefix.
                     Query: {query}'''
        title = self.model.generate(prompt)
        return title

    def generate_summary(self, graph, request, user_query=None,graph_id=None, summary=None):
        if self.model is None:
            return "No summary available"
        summarizer = Graph_Summarizer(self.model)
        summary = summarizer.summary(graph, request, user_query, graph_id, summary)
        return summary
