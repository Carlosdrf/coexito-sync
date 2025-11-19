# COEXITO
import os
import time
import requests
import base64
from io import BytesIO
from PIL import Image
from dotenv import load_dotenv

load_dotenv()

PROVIDING_PENDING = 'not provided'

COMPANY_ID=os.getenv('COMPANY_ID')
WEBSITE_ID=os.getenv('WEBSITE_ID')
BASE_URL=os.getenv('BASE_URL')
COMPANY=os.getenv('COMPANY')

BASE_WEBHOOK_URL=os.getenv('BASE_WEBHOOK_URL')
PRODUCTS_WEBHOOK_ID=os.getenv('PRODUCTS_WEBHOOK_ID')
CLIENTS_WEBHOOK_ID=os.getenv('CLIENTS_WEBHOOK_ID')
PRICE_LISTS_WEBHOOK_ID=os.getenv('PRICE_LISTS_WEBHOOK_ID')
STOCK_WEBHOOK_ID=os.getenv('STOCK_WEBHOOK_ID')
STOCK_LOCATIONS_WEBHOOK_ID=os.getenv('STOCK_LOCATIONS_WEBHOOK_ID')

# fields map config
additional_fields = {
    "company_id": COMPANY_ID,
    "website_id": WEBSITE_ID,
}

item_fields = {
    'additional_fields': {
        "sale_ok": True,
        "detailed_type": "product",
        'taxes_id': [50],
        'price': 0,
        'active': True,
        **additional_fields,
    },
    'mapping': [
        {
            "sourceId": "productId",
            "field": "default_code"
        },
        {
            "sourceId": "name",
            "field": "name",
        },
        {
            "sourceId": "brand",
            "field": "brand",
        },
        {
            "sourceId": "category",
            "field": "categ_id",
        },
        {
            'sourceId': 'productImageUrl',
            'field': 'product_image_url',
        },
    ]
}
session = requests.Session()

session.headers.update({
    "x-api-key": os.getenv('COEXITO_API_KEY')
})

webhook_session = requests.Session()

webhook_session.headers.update({
    "x-api-key": os.getenv('WEBHOOK_CLIENT_API')
})

# get items response
def listItems(s: requests, limit = None, page = None):
    url = "products"
    size = None if limit is None else f"size={limit}"
    pagination = None if page is None else f"page={page}"
    params = list(filter(lambda x: x is not None, [
        size,
        pagination,
    ]))

    queryFilter = ('&').join(params)
    return s.get(f"{BASE_URL}/{url}?{queryFilter}")

# get odoo field name from the source field name if any
def getField(name, mapping_fields):
    for m in mapping_fields:
        if m.get('sourceId') == name:
            return m.get('field')
    return None

# check if the field is nested or simple field
def checkNestedField(field, items, mapping_fields):
    response = []
    for m in mapping_fields:
        if m.get('sourceId') in field and m.get('fields') is not None:
            for item in items:
                response.append(mapFieldNames(item, m.get('fields')))
            return response
    return items


# replace source attribute fields to odoo required attribute naming convention
def mapFieldNames(item, mapping_fields, additional_fields = None):
    new_keys = {}

    for k in item.keys():
        key = getField(k, mapping_fields)
        if key is not None:
            new_keys[key] = "Not available" if item[k] is None else checkNestedField(k, item[k], mapping_fields)
    
    if additional_fields is not None:
        new_keys = {
            **new_keys,
            **additional_fields,
        }
    return new_keys

# transform source fields into odoo required fields structure
def processFields(s: requests, response: requests.Response, fields, additional_fields = None):
    mappedValues = []
    items = response.json().get('content', [])

    for item in items:
        mappedValues.append(mapFieldNames(item, fields, additional_fields))
    # print(mappedValues)
    return {
        "count": len(items),
        "brand": COMPANY,
        "data": mappedValues,
        # "_model": 'clients',
        # "_id": '1'
    }    

def encode_url_to_base64(image_url: str):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "image/png,image/jpeg,*/*"
    }

    response = requests.get(image_url, headers=headers)
    response.raise_for_status()

    encoded = base64.b64encode(response.content)
    image_send = encoded.decode('ascii')

    try:
        base64.b64decode(image_send)
    except Exception as e:
        print(f'error in validation: {e}')
    # save_debug_images(image_send, response.content)    

    return image_send

def save_debug_images(decoded_image: str, content: bytes):
    with open("./archivo_codificado.png", "wb") as f:
        f.write(content)
        print("Contenido guardado en 'archivo_codificado.png'")
    with open("./debug_codificado.png", "wb") as f:
        f.write(base64.b64decode(decoded_image))
        print("Contenido guardado en 'archivo_codificado.png'")


def sendProductsData():
    next_link = 1
    while True:
        products_response = listItems(session, page=next_link)
        payload = processFields(
            session,
            products_response,
            item_fields.get('mapping'),
            item_fields.get('additional_fields')
        )

        items = []
        i = 0
        for index, item in enumerate(payload.get('data')):
            i = i + 1
            if item['product_image_url'] is not None and item['product_image_url'] != '':
                item['product_image_url'] = None if item['product_image_url'] == '' else encode_url_to_base64(item['product_image_url'])
                items.append(item)
            if i == 10 or index == len(payload.get('data')):
                i = 0
                streamData(session, PRODUCTS_WEBHOOK_ID, payload={
                    "model": 'Product Images',
                    "count": len(items),
                    "data": items
                })
                items=[]

                time.sleep(0.6)
        
        next_link = products_response.json().get('number') + 1
        total_pages = products_response.json().get('totalPages')
        if next_link > total_pages:
            break

# send the data to the webhook
def streamData(s: requests, endpoint, payload: dict[str, any]):
    try: 
        print('sending request...')
        print(f'{BASE_WEBHOOK_URL}/{endpoint}')
        request = webhook_session.post(f"{BASE_WEBHOOK_URL}/{endpoint}", json=payload)

        # request.raise_for_status()
        print(request.json())
        return request
    except requests.HTTPError as e:
        print('http error')
        print(e)
    except requests.exceptions.RequestException as e:
        print('exception')
        print(e)
    except Exception as e:
        print(f'some other fail reason: {e}')

sendProductsData()
