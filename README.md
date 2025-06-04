# yelp-taco-enumerator

a tool that finds, analyzes, and catalogs taco restaurants using the yelp api and llm technology.

## overview

this project automatically discovers taco restaurants in a specified location using the yelp fusion api, analyzes their reviews with an llm (claude or gpt) to determine each restaurant's best taco offering, and stores all this information in a local sqlite database for easy querying and analysis.

## features

- automatically searches for taco restaurants in a specified location (default: austin, tx)
- fetches detailed information including business hours and reviews
- uses llm technology to analyze review text and identify the "best taco" at each restaurant
- stores all data in a local sqlite database for persistence
- provides tools to view and analyze the collected data
- includes a pythonic interface (tacodb.py) for developers to easily interact with the database
- supports pagination and handles yelp api rate limits
- tracks progress to allow resuming searches from where they left off

## requirements

- python 3.6+
- yelp fusion api key
- openai api key or openrouter api key (for llm analysis)
- required python packages:
  - requests
  - pandas
  - openai>=1.0.0
  - sqlite3

## setup

1. clone this repository
2. install required packages:
   ```
   pip install requests pandas openai
   ```
3. set up environment variables with your api keys:
   ```
   export YELP_API_KEY="your_yelp_api_key"
   export OPENAI_API_KEY="your_openai_api_key"  # if using openai directly
   export OPENROUTER_API_KEY="your_openrouter_api_key"  # if using openrouter
   ```

## usage

### finding taco restaurants

run the main script to search for and analyze taco restaurants:

```
python find-taco.py
```

add the `--debug` flag for more detailed logging:

```
python find-taco.py --debug
```

alternatively, use the provided shell script which sets up environment variables:

```
./run.sh
```

### viewing the data

to view all restaurants in the database:

```
python view_db.py
```

to view a specific restaurant with its reviews:

```
python view_db.py --id [restaurant_id]
```

to display statistics about the collected data:

```
python view_db.py --stats
```

### using the python interface (tacodb.py)

the project includes a pythonic interface for developers to easily read and analyze the taco database:

```python
import tacodb

# get all restaurants
restaurants = tacodb.get_all_restaurants()
for restaurant in restaurants[:3]:  # show first 3
    print(f"{restaurant['name']} - Best taco: {restaurant['best_taco']}")

# get a specific restaurant with all its details
restaurant = tacodb.get_restaurant_by_id("some-yelp-id")
if restaurant:
    print(f"Name: {restaurant['name']}")
    print(f"Address: {restaurant['address']}")
    print(f"Best taco: {restaurant['best_taco']}")

    # hours are parsed into a dictionary
    for day, hours in restaurant['hours'].items():
        print(f"{day}: {hours or 'Closed'}")

# get all reviews for a restaurant
reviews = tacodb.get_reviews_for_restaurant("some-yelp-id")
for review in reviews:
    print(f"Rating: {review['rating']}/5 - {review['text']}")

# search for restaurants by name or address
results = tacodb.search_restaurants("torchy's")
print(f"Found {len(results)} restaurants matching 'torchy's'")

# find restaurants with a specific taco
al_pastor_places = tacodb.get_restaurants_by_taco("al pastor")
print(f"Found {len(al_pastor_places)} restaurants with Al Pastor tacos")

# get the most popular tacos
popular_tacos = tacodb.get_popular_tacos(limit=5)
for taco, count in popular_tacos:
    print(f"{taco}: found at {count} restaurants")
```

for more advanced queries, you can instantiate the TacoDB class:

```python
from tacodb import TacoDB

# create a db instance (optionally with custom path)
db = TacoDB(db_path="taco_restaurants.db")

# get total number of restaurants
count = db.get_restaurant_count()
print(f"Database contains {count} taco restaurants")

# get restaurants by zip code
zip_stats = db.get_restaurants_by_zip()
for zip_code, count in sorted(zip_stats.items(), key=lambda x: x[1], reverse=True)[:5]:
    print(f"Zip {zip_code}: {count} restaurants")

# get a setting from the database
last_offset = db.get_setting("last_offset", "0")
print(f"Last search offset: {last_offset}")
```

## configuration

you can modify the following variables in `find-taco.py` to customize the search:

- `LOCATION`: the location to search for taco restaurants (default: "austin, tx")
- `TERM`: the search term (default: "taco")
- `LIMIT_PER_PAGE`: number of results per page (default: 50, max allowed by yelp)
- `USE_OPENROUTER`: toggle between openrouter and openai (default: true)

## how it works

1. the script searches for taco restaurants using the yelp fusion api
2. for each restaurant, it fetches detailed information including business hours
3. it then retrieves reviews for each restaurant
4. an llm (claude or gpt) analyzes the review text to determine the "best taco" at each restaurant
5. all information is stored in a sqlite database
6. the script tracks its progress, allowing it to resume from where it left off in subsequent runs

## database schema

the sqlite database contains the following tables:

- `taco_restaurants`: stores restaurant information
  - id: yelp business id
  - name: restaurant name
  - address: restaurant address
  - hours: business hours in json format
  - best_taco: the identified best taco offering

- `reviews`: stores review information
  - id: auto-incremented review id
  - restaurant_id: foreign key to taco_restaurants
  - text: review text
  - rating: review rating (1-5)
  - date: review date

- `app_settings`: stores application settings
  - key: setting name
  - value: setting value

## tacodb interface reference

the `tacodb.py` module provides a pythonic interface to the taco database with the following functionality:

### module-level functions (read operations)

these functions can be used directly without instantiating the class:

- `get_all_restaurants()`: returns all restaurants as a list of dictionaries with keys:
  ```
  {
    'id': 'yelp-business-id',
    'name': 'Restaurant Name',
    'address': '123 Main St, Austin, TX 78701',
    'hours': {'Mon': '11:00-22:00', 'Tue': '11:00-22:00', ...},
    'best_taco': 'Al Pastor Taco'
  }
  ```

- `get_restaurant_by_id(restaurant_id)`: returns a specific restaurant by its yelp id

- `get_reviews_for_restaurant(restaurant_id)`: returns all reviews for a restaurant as a list of dictionaries:
  ```
  [
    {'id': 1, 'text': 'Great tacos!', 'rating': 5.0, 'date': '2025-01-15'},
    ...
  ]
  ```

- `search_restaurants(query)`: searches restaurants by name or address

- `get_restaurants_by_taco(taco_type)`: finds restaurants with a specific taco type

- `get_popular_tacos(limit=10)`: returns the most popular taco types as a list of tuples:
  ```
  [('Al Pastor Taco', 15), ('Carnitas Taco', 12), ...]
  ```

### tacodb class (advanced operations)

for more advanced queries, use the `TacoDB` class:

```python
from tacodb import TacoDB
db = TacoDB(db_path="taco_restaurants.db")  # path is optional
```

the class provides all the read functionality of the module-level functions plus:

- `get_restaurant_count()`: returns the total number of restaurants
- `get_setting(key, default_value)`: retrieves a setting from app_settings
- `get_restaurants_by_zip()`: returns a dictionary of zip codes and restaurant counts

the class also provides write operations:
- `set_setting(key, value)`: stores a setting in app_settings
- `add_restaurant(...)`: adds a new restaurant to the database
- `add_review(...)`: adds a new review to the database
- `delete_restaurant(restaurant_id)`: deletes a restaurant and its reviews

## limitations

- the yelp api deliberately truncates review text with "..." - this is a limitation of the api and cannot be bypassed without violating yelp's terms of service
- yelp api has rate limits that may affect how quickly data can be collected
- llm analysis of the "best taco" is based on available review text and may not always be accurate

## license

this project is for educational purposes only. use responsibly and in accordance with yelp's terms of service.