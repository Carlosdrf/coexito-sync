# COEXITO
import os
import requests

from dotenv import load_dotenv

load_dotenv()

PROVIDING_PENDING = 'not provided'

COMPANY_ID=os.getenv('COMPANY_ID')
WEBSITE_ID=os.getenv('WEBSITE_ID')
BASE_URL = os.getenv('BASE_URL')
COMPANY = os.getenv('COMPANY')

BASE_WEBHOOK_URL=os.getenv('BASE_WEBHOOK_URL')
PRODUCTS_WEBHOOK_ID=os.getenv('PRODUCTS_WEBHOOK_ID')
CLIENTS_WEBHOOK_ID=os.getenv('CLIENTS_WEBHOOK_ID')
PRICE_LISTS_WEBHOOK_ID=os.getenv('PRICE_LISTS_WEBHOOK_ID')
STOCK_WEBHOOK_ID=os.getenv('STOCK_WEBHOOK_ID')
STOCK_LOCATIONS_WEBHOOK_ID=os.getenv('STOCK_LOCATIONS_WEBHOOK_ID')

# handler 
def handler(event = None, context = None):
    stocks = sendStockLocationData()
    # sendProductsData()
    sendClientsData(stocks)
    # sendPriceListData()
    # sendStockQuantityData()

session = requests.Session()

session.headers.update({
    "x-api-key": os.getenv('COEXITO_API_KEY')
})

webhook_session = requests.Session()

webhook_session.headers.update({
    "x-api-key": os.getenv('WEBHOOK_CLIENT_API')
})

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
    ]
}

client_fields = {
    "additional_fields": {
        'company_type': 'individual',
        **additional_fields,
    },
    "mapping": [
        {
            "sourceId": "name",
            "field": "name",
        },
        {
            "sourceId": "phone",
            "field": "phone",
        },
        {
            "sourceId": "email",
            "field": "email",
        },
        {
            'sourceId': 'commerceId',
            'field': 'commerce_id',
        },
        {
            'sourceId': 'globalCommerceId',
            'field': 'vat'
        },
        {
            'sourceId': 'salesCenter',
            'field': 'sales_center',
        },
        {
            'sourceId': 'distributionCenterIds',
            'field': 'distribution_center_id',
        },
        {
            "sourceId": "typeListId",
            "field": "property_product_pricelist",
        },
        {
            'sourceId': 'sellerIds',
            'field': 'seller_id',
        },
        {
            "sourceId": "locations",
            "field": "locations",
            "fields": [
                {
                    "sourceId": "country",
                    "field": "country",
                },
                {
                    "sourceId": "region",
                    "field": "state_id",
                },
                {
                    "sourceId": "city",
                    "field": "city",
                },
                {
                    "sourceId": "address",
                    "field": "street",
                },
                {
                    "sourceId": "postalCode",
                    "field": "zip",
                },
            ]
        },
    ]
}

price_list_fields = {
    'additional_fields': None,
    'mapping': [
        {
            "sourceId": "name",
            "field": "name",
        },
        {
            "sourceId": "productId",
            "field": "product_tmpl_id",
        },
        {
            "sourceId": "pricePerUnit",
            "field": "fixed_price",
        },
        {
            "sourceId": "typeListId",
            "field": "listId",
        },
    ]
}

stock_fields = {
    'mapping': [
        {
            "sourceId": "distributionCenterId",
            "field": "location_id",
        },
        {
            "sourceId": "distributionCenterName",
            "field": "name",
        },
        {
            "sourceId": "productId",
            "field": "product_id",
        },
        {
            "sourceId": "quantity",
            "field": "inventory_quantity_auto_apply",
        },
    ]
}

stock_location_fields = {
    'mapping': [
        {
            "sourceId": "distributionCenterId",
            "field": "center_id",
        },
        {
            "sourceId": "name",
            "field": "name",
        },
    ]
}

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
    print(queryFilter)
    return s.get(f"{BASE_URL}/{url}?{queryFilter}")

#get clients response
def listClients(page):
    print(f"page={page}")
    endpoint = 'commerces'
    return session.get(f"{BASE_URL}/{endpoint}?page={page}")

# credit limit
def listCreditLimit(page):
    endpoint = 'commerces/credit-limit'
    return session.get(f"{BASE_URL}/{endpoint}?page={page}")

# get price list response
def getPriceLists(page):
    endpoint = 'price-lists'
    return session.get(f"{BASE_URL}/{endpoint}?page={page}")

def getStocks(page):
    endpoint = 'stocks'
    return request(endpoint, page)

def getStocksLocation(page):
    endpoint = 'distribution-centers'
    return request(endpoint, page)

def request(endpoint, page):
    return session.get(f"{BASE_URL}/{endpoint}?page={page}")

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

# send the data to the webhook
def streamData(s: requests, endpoint, payload: dict[str, any]):
    try: 
        print('sending request...')
        print(f'{BASE_WEBHOOK_URL}/{endpoint}')
        
        request = webhook_session.post(f"{BASE_WEBHOOK_URL}/{endpoint}", json=payload)

        request.raise_for_status()
        print(request.json())
        return request
    except requests.exceptions.RequestException as e:
        print('exception')
        print(e)
    except:
        print('some other fail reason')


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

        streamData(session, PRODUCTS_WEBHOOK_ID, payload)
        
        next_link = products_response.json().get('number') + 1
        total_pages = products_response.json().get('totalPages')
        if next_link > total_pages:
            break

def getCreditLimit():
    next_link = 1
    data = []
    while True:
        credit_limit = listCreditLimit(next_link)
        data = [
            *data,
            *credit_limit.json().get('content')
        ]

        next_link = credit_limit.json().get('number') + 1
        total_pages = credit_limit.json().get('totalPages')

        if next_link > total_pages:
            break

    return data

def assignCreditLimit(credit_limit, commerce_id):
    for credit in credit_limit:
        if credit.get('commerceId') == commerce_id:
            return credit.get('maxAmount') - credit.get('balance')

    return 0

def sendClientsData(stocks: list):
    credit_limit = getCreditLimit()
    print(len(credit_limit))
    next_link = 1
    payload = []
    while True:
        clients_response = listClients(next_link)
        response = processFields(
            session,
            clients_response,
            client_fields.get('mapping'), 
            client_fields.get('additional_fields'),
        )

        # print(response.get('data'))

        payload = [
            *payload,
            *response.get('data'),
        ]

        next_link = clients_response.json().get('number') + 1
        total_pages = clients_response.json().get('totalPages')

        grouped = {}

        for item in response.get('data'):
            key = item['name']
            if key not in grouped:
                grouped[key] = {
                    **item,
                    'credit': assignCreditLimit(credit_limit, item['commerce_id'])
                }
            else:
                grouped[key]['locations'].append(*item.get('locations'))
            grouped[key]['seller_id'] = item['seller_id'][0]
            grouped[key]['distribution_center_id'] = item['distribution_center_id'][0]
            
            for stock in stocks:
                if grouped[key]['distribution_center_id'] == stock.get('center_id'):
                    grouped[key]['distribution_center_id'] = stock.get('name')


        streamData(session, CLIENTS_WEBHOOK_ID, payload={
            'count': len(list(grouped.values())),
            "brand": COMPANY,
            "data": list(grouped.values())
        })
        if next_link > total_pages:
            break

    

    # for item in payload.get('data'):
    #     item['name']
        

def groupPriceListData(items):
    grouped = {}
    for item in items:
        key = item['listId']
        if key not in grouped:
            grouped[key] = {
                "listId": item['listId'],
                "name": item['name'],
                "currency_id": "COP",
                "discount_policy": "without_discount",
                "lines": [],
                **additional_fields,
            }
        grouped[key]['lines'].append({
            "listId": item['listId'],
            'product_tmpl_id': item['product_tmpl_id'],
            'name': item.get('name'),
            "currency_id": "COP",
            'fixed_price': item.get('fixed_price', 0.0)
        })
    return grouped

def sendPriceListData():
    next_link = 1
    payload = []
    while True:
        price_list_response = getPriceLists(next_link)
        response = processFields(
            session,
            price_list_response,
            price_list_fields.get('mapping'),
            price_list_fields.get('additional_fields'),
        )
        total_pages = price_list_response.json().get('totalPages')
        next_link = price_list_response.json().get('number') + 1
        payload = [
            *payload,
            *response.get('data')
        ]
        if next_link > total_pages:
            break

    grouped = groupPriceListData(payload)
    # print(list(grouped.values()))

    streamData(session, PRICE_LISTS_WEBHOOK_ID, payload={
        "entity": 'Price Lists',
        "brand": COMPANY,
        "data": list(grouped.values()),
    })
    print(len(list(grouped.values())))

def sendStockQuantityData():
    next_link = 1
    payload = []
    while True:
        stock_response = getStocks(next_link)
        response = processFields(
            session,
            stock_response,
            stock_fields.get('mapping'),
            )
        total_pages = stock_response.json().get('totalPages')
        next_link = stock_response.json().get('number') + 1
        # print(response)
        payload = [
            *payload,
            *response.get('data'),
        ]
        streamData(session, STOCK_WEBHOOK_ID, response)
        if next_link > total_pages:
            break

def processRequestData(
        executeFunc: callable,
        mapping_fields,
        entity,
    ):
    next_link = 1
    payload = []
    while True:
        req_response = executeFunc(next_link)
        response = processFields(
            session,
            req_response,
            mapping_fields.get('mapping')
            )
        total_pages = req_response.json().get('totalPages')
        next_link = req_response.json().get('number') + 1

        payload = [
            *payload,
            *response.get('data'),
        ]
        
        # streamData(session, entity, response)
        if next_link > total_pages:
            break
    return payload

def sendStockLocationData():
    return processRequestData(
        getStocksLocation,
        stock_location_fields,
        STOCK_LOCATIONS_WEBHOOK_ID,
        )

handler()

