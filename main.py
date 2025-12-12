import os
import json
import requests
import redis
from google.cloud import pubsub_v1
from llama_cpp import Llama

class MemeTextGenerator:
    def __init__(self, model_path="./model.gguf"):
        if not os.path.exists(model_path):
             model_path = "./model.gguf" 
             
        self.llm = Llama(model_path=model_path, n_ctx=512, verbose=False)

    def generate_text(self, topic):
        prompt = f"""<|im_start|>system
You are a funny bot. Your goal is to tell a short joke about the user's topic.
Do not repeat yourself. Keep it very short.<|im_end|>
<|im_start|>user
Write a short joke about: {topic}<|im_end|>
<|im_start|>assistant
"""
        try:
            print(f"Generating text for topic: {topic}")
            output = self.llm.create_completion(
                prompt, 
                max_tokens=128,
                temperature=0.6,
                repeat_penalty=1.3,
                stop=["<|im_end|>", "<|endoftext|>"]
            )
            return output['choices'][0]['text'].strip()
        except:
            return "Error generating text"

def send_to_discord(webhook_url, content):
    try:
        data = {"content": content}
        response = requests.post(webhook_url, json=data)
        response.raise_for_status()
        print(f"Sent to Discord: {response.status_code}")
    except Exception as e:
        print(f"Error sending to Discord: {e}")

def callback(message):
    print(f"Received message: {message.data}")
    
    try:
        data = json.loads(message.data.decode('utf-8'))
        instruction = data.get('instruction', '')
        
        print(f"Processing instruction: {instruction}")
        
        cached_meme = redis_client.get(instruction)
        if cached_meme:
            meme_text = cached_meme.decode('utf-8')
            print(f"Using cached meme: {meme_text}")
        else:
            meme_text = generator.generate_text(instruction)
            redis_client.set(instruction, meme_text)
            print(f"Generated meme: {meme_text}")
        
        webhook_url = os.environ.get('DISCORD_URL')
        if webhook_url:
            send_to_discord(webhook_url, meme_text)
        else:
            print("Warning: DISCORD_URL not set")
        
        message.ack()
        
    except Exception as e:
        print(f"Error processing message: {e}")
        message.nack()

def main():
    global redis_client
    
    redis_host = os.environ.get('REDIS_HOST')
    redis_port = int(os.environ.get('REDIS_PORT', 6379))
    redis_auth_string = os.environ.get('REDIS_AUTH_STRING')

    print(f"Connecting to Redis at {redis_host}:{redis_port}...")
    
    redis_client = redis.Redis(
        host=redis_host, 
        port=redis_port, 
        password=redis_auth_string, 
        decode_responses=False,
        socket_connect_timeout=5,
        socket_timeout=5,
        retry_on_timeout=True
    )
    
    try:
        redis_client.ping()
        print("Successfully connected to Redis")
    except Exception as e:
        print(f"Failed to connect to Redis: {e}")
        return
    
    project_id = os.environ.get('GCP_PROJECT_ID')
    subscription_id = os.environ.get('PUBSUB_SUBSCRIPTION_ID')
    
    if not project_id or not subscription_id:
        print("Error: GCP_PROJECT_ID and PUBSUB_SUBSCRIPTION_ID must be set")
        return
    
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    
    print(f"Listening to: {subscription_path}")
    
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    
    print("Listening for messages on Pub/Sub...")
    
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("Stopped listening")


generator = MemeTextGenerator()

if __name__ == "__main__":
    main()