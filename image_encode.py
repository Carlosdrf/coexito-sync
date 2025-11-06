import requests
import base64

def encode_url_to_base64(image_url: str):
    response = requests.get(image_url)
    response.raise_for_status()

    encoded = base64.b64encode(response.content).decode('utf-8')
    return encoded