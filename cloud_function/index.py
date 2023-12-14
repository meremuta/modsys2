
import json
from functions import *




def handler(event, context):
    response = json.loads(json.dumps(event))
    method = response['httpMethod']
    path = response['path']

    if path == '/register':
        result = register(response, method)
    elif path == '/search':
        result = parse_search(response)
    elif path == '/addToWishlist':
        result = add_to_wishlist(response, method)
    elif path == '/getWishlist':
        result = return_user_wishlist(response, method)
    elif path == '/getWatchedlist':
        result = return_user_watchlist(response, method)
    elif path == '/deleteWish':
        result = delete_from_wishlist(response, method)
    elif path == '/deleteWatched':
        result = delete_from_watchedlist(response, method)
    elif path == '/postWatched':
        result = add_to_watched(response, method)
    elif path == '/recom':
        result = random_recom(response, method)
    elif path == '/remind':
        result = remind(response, method)
    else:
        result = 'Dont recognize path: ' + path


    print(event)
    return {
        'statusCode': 200,
        'body': result,
    }