import os
import threading
from google.oauth2 import service_account
from google.cloud import aiplatform

# Global state for Vertex AI initialization
_vertex_initialized = False
_vertex_lock = threading.Lock()
_vertex_error = None

def _initialize_vertex_ai():
    """Initialize Vertex AI once (thread-safe)"""
    global _vertex_initialized, _vertex_error
    
    if _vertex_initialized:
        return _vertex_error is None
    
    with _vertex_lock:
        if _vertex_initialized:
            return _vertex_error is None
        
        try:
            service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            if not service_account_path:
                _vertex_error = "GOOGLE_APPLICATION_CREDENTIALS not set"
                _vertex_initialized = True
                return False
            
            if not os.path.exists(service_account_path):
                _vertex_error = f"Credentials file not found: {service_account_path}"
                _vertex_initialized = True
                return False
            
            project_id = os.getenv("GCP_PROJECT_ID")
            if not project_id:
                _vertex_error = "GCP_PROJECT_ID not set"
                _vertex_initialized = True
                return False
            
            region = os.getenv("GCP_REGION", "us-central1")
            
            credentials = service_account.Credentials.from_service_account_file(service_account_path)
            aiplatform.init(project=project_id, location=region, credentials=credentials)
            
            _vertex_error = None
            _vertex_initialized = True
            print(f"✅ Vertex AI initialized: project={project_id}, region={region}")
            return True
            
        except Exception as e:
            _vertex_error = str(e)
            _vertex_initialized = True
            print(f"❌ Vertex AI initialization failed: {e}")
            return False

# Loads Gemini and returns a short explanation for a trading signal
# Inputs: symbol, price, sentiment, news (list of dicts), price_change, signal
def get_gemini_explanation(symbol, price, sentiment, news, price_change, signal):
    # Initialize Vertex AI if not already done
    if not _initialize_vertex_ai():
        return f"[Vertex AI not configured: {_vertex_error}]"
    
    try:
        from vertexai.preview.language_models import ChatModel
        
        # Compose a prompt for Gemini
        headlines = '\n'.join([f"- {n.get('title', '')}" for n in news[:3]]) if news else "No recent news."
        prompt = f"""You are a financial AI assistant. Given the following data, explain in 1-2 sentences why the stock received a {signal} signal. Be concise and use plain English.

Symbol: {symbol}
Price: {price}
Price Change: {price_change}
Sentiment Score: {sentiment}
Recent News Headlines:
{headlines}"""
        
        # Try different model names (API might have changed)
        model_names = [
            "gemini-1.5-flash",
            "gemini-1.5-flash-preview",
            "gemini-1.0-pro",
            "gemini-pro"
        ]
        
        for model_name in model_names:
            try:
                chat_model = ChatModel.from_pretrained(model_name)
                chat = chat_model.start_chat()
                response = chat.send_message(prompt)
                result = response.text.strip()
                if result:
                    return result
            except Exception as model_error:
                if model_name == model_names[-1]:  # Last model, raise the error
                    raise model_error
                continue  # Try next model
        
        return "[Vertex AI: No response from models]"
        
    except ImportError as e:
        return f"[Vertex AI: Import error - {e}. Install: pip install google-cloud-aiplatform]"
    except Exception as e:
        error_msg = str(e)
        # Don't expose full error to frontend, just log it
        print(f"[ERROR] Vertex AI call failed: {error_msg}")
        return f"[Vertex AI error: {error_msg[:100]}]"
