import os
from vertexai.preview.language_models import ChatModel, InputOutputTextPair
from google.oauth2 import service_account
from google.cloud import aiplatform

# Loads Gemini and returns a short explanation for a trading signal
# Inputs: symbol, price, sentiment, news (list of dicts)
def get_gemini_explanation(symbol, price, sentiment, news, price_change, signal):
    # Compose a prompt for Gemini
    headlines = '\n'.join([f"- {n.get('title', '')}" for n in news[:3]]) if news else "No recent news."
    prompt = f"""
    You are a financial AI assistant. Given the following data, explain in 1-2 sentences why the stock received a {signal} signal. Be concise and use plain English.
    
    Symbol: {symbol}
    Price: {price}
    Price Change: {price_change}
    Sentiment Score: {sentiment}
    Recent News Headlines:\n{headlines}
    """
    
    # Authenticate using service account JSON (path from env)
    service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not service_account_path or not os.path.exists(service_account_path):
        return "[Vertex AI not configured: missing credentials]"
    credentials = service_account.Credentials.from_service_account_file(service_account_path)
    aiplatform.init(project=os.getenv("GCP_PROJECT_ID"), location=os.getenv("GCP_REGION", "us-central1"), credentials=credentials)
    
    # Use Gemini 1.5 Flash (or default)
    chat_model = ChatModel.from_pretrained("gemini-1.5-flash-preview")
    chat = chat_model.start_chat()
    try:
        response = chat.send_message(prompt)
        return response.text.strip()
    except Exception as e:
        return f"[Vertex AI error: {e}]"
