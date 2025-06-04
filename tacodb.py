import sqlite3
import json
import os
from typing import List, Dict, Tuple, Optional, Any, Union

# database path
DB_PATH = "taco_restaurants.db"

class TacoDB:
    """
    a pythonic interface for the taco restaurant database.
    provides methods to query and manipulate taco restaurant data.
    """

    def __init__(self, db_path: str = DB_PATH):
        """
        initialize the tacodb interface with the specified database path.

        args:
            db_path: path to the sqlite database file.
        """

        self.db_path = db_path
        self._ensure_db_exists()

    def _ensure_db_exists(self) -> None:
        """
        ensure the database exists and has the required tables.
        creates the database and tables if they don't exist.

        returns:
            none.
        """

        if not os.path.isfile(self.db_path):
            self._init_database()

    def _init_database(self) -> None:
        """
        initialize the sqlite database with required tables.
        creates the taco_restaurants, reviews, and app_settings tables.

        returns:
            none.
        """

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # create restaurants table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS taco_restaurants (
            id TEXT PRIMARY KEY,
            name TEXT,
            address TEXT,
            hours TEXT,
            best_taco TEXT
        )
        ''')

        # create reviews table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            restaurant_id TEXT NOT NULL,
            text TEXT NOT NULL,
            rating REAL,
            date TEXT,
            FOREIGN KEY (restaurant_id) REFERENCES taco_restaurants(id),
            UNIQUE(text)
        )
        ''')

        # create app_settings table for storing key-value pairs
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        ''')

        conn.commit()
        conn.close()

    def get_restaurant_count(self) -> int:
        """
        get the total count of restaurants in the database.

        returns:
            int: the number of restaurants in the database.
        """

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM taco_restaurants")
            count = cursor.fetchone()[0]
            conn.close()
            return count
        except Exception as e:
            print(f"error counting restaurants in database: {e}")
            return 0

    def get_all_restaurants(self) -> List[Dict[str, Any]]:
        """
        get all restaurants from the database.

        returns:
            list: a list of dictionaries containing restaurant data.
        """

        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("""
            SELECT id, name, address, hours, best_taco
            FROM taco_restaurants
            """)

            restaurants = []
            for row in cursor.fetchall():
                restaurant = dict(row)
                # parse hours json string to dictionary
                try:
                    restaurant['hours'] = json.loads(restaurant['hours'])
                except (json.JSONDecodeError, TypeError):
                    restaurant['hours'] = None

                restaurants.append(restaurant)

            conn.close()
            return restaurants
        except Exception as e:
            print(f"error fetching restaurants: {e}")
            return []

    def get_restaurant_by_id(self, restaurant_id: str) -> Optional[Dict[str, Any]]:
        """
        get a specific restaurant by its id.

        args:
            restaurant_id: the yelp business id of the restaurant.

        returns:
            dict: restaurant data or none if not found.
        """

        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("""
            SELECT id, name, address, hours, best_taco
            FROM taco_restaurants
            WHERE id = ?
            """, (restaurant_id,))

            row = cursor.fetchone()
            conn.close()

            if row:
                restaurant = dict(row)
                # parse hours json string to dictionary
                try:
                    restaurant['hours'] = json.loads(restaurant['hours'])
                except (json.JSONDecodeError, TypeError):
                    restaurant['hours'] = None

                return restaurant
            return None
        except Exception as e:
            print(f"error fetching restaurant {restaurant_id}: {e}")
            return None

    def get_reviews_for_restaurant(self, restaurant_id: str) -> List[Dict[str, Any]]:
        """
        get all reviews for a specific restaurant.

        args:
            restaurant_id: the yelp business id of the restaurant.

        returns:
            list: a list of dictionaries containing review data.
        """

        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("""
            SELECT id, text, rating, date
            FROM reviews
            WHERE restaurant_id = ?
            ORDER BY date DESC
            """, (restaurant_id,))

            reviews = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return reviews
        except Exception as e:
            print(f"error fetching reviews for restaurant {restaurant_id}: {e}")
            return []

    def search_restaurants(self, query: str) -> List[Dict[str, Any]]:
        """
        search for restaurants by name or address.

        args:
            query: the search query string.

        returns:
            list: a list of dictionaries containing matching restaurant data.
        """

        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # use wildcards for partial matching
            search_param = f"%{query}%"

            cursor.execute("""
            SELECT id, name, address, hours, best_taco
            FROM taco_restaurants
            WHERE name LIKE ? OR address LIKE ?
            """, (search_param, search_param))

            restaurants = []
            for row in cursor.fetchall():
                restaurant = dict(row)
                # parse hours json string to dictionary
                try:
                    restaurant['hours'] = json.loads(restaurant['hours'])
                except (json.JSONDecodeError, TypeError):
                    restaurant['hours'] = None

                restaurants.append(restaurant)

            conn.close()
            return restaurants
        except Exception as e:
            print(f"error searching restaurants: {e}")
            return []

    def get_restaurants_by_taco(self, taco_type: str) -> List[Dict[str, Any]]:
        """
        get restaurants that have a specific best taco.

        args:
            taco_type: the type of taco to search for.

        returns:
            list: a list of dictionaries containing matching restaurant data.
        """

        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # use wildcards for partial matching
            search_param = f"%{taco_type}%"

            cursor.execute("""
            SELECT id, name, address, hours, best_taco
            FROM taco_restaurants
            WHERE best_taco LIKE ?
            """, (search_param,))

            restaurants = []
            for row in cursor.fetchall():
                restaurant = dict(row)
                # parse hours json string to dictionary
                try:
                    restaurant['hours'] = json.loads(restaurant['hours'])
                except (json.JSONDecodeError, TypeError):
                    restaurant['hours'] = None

                restaurants.append(restaurant)

            conn.close()
            return restaurants
        except Exception as e:
            print(f"error fetching restaurants by taco type: {e}")
            return []

    def get_popular_tacos(self, limit: int = 10) -> List[Tuple[str, int]]:
        """
        get the most popular taco types based on frequency.

        args:
            limit: maximum number of results to return.

        returns:
            list: a list of tuples containing (taco_type, count).
        """

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("""
            SELECT best_taco, COUNT(*) as count
            FROM taco_restaurants
            WHERE best_taco IS NOT NULL AND best_taco != '' AND best_taco != 'Unknown'
            GROUP BY best_taco
            ORDER BY count DESC
            LIMIT ?
            """, (limit,))

            popular_tacos = cursor.fetchall()
            conn.close()
            return popular_tacos
        except Exception as e:
            print(f"error fetching popular tacos: {e}")
            return []

    def get_setting(self, key: str, default_value: str = "") -> str:
        """
        get a setting value from the app_settings table.

        args:
            key: the setting key.
            default_value: value to return if key doesn't exist.

        returns:
            str: the setting value or default if not found.
        """

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM app_settings WHERE key = ?", (key,))
            result = cursor.fetchone()
            conn.close()

            if result:
                return result[0]
            return default_value
        except Exception as e:
            print(f"error getting setting {key}: {e}")
            return default_value

    def set_setting(self, key: str, value: str) -> bool:
        """
        store a setting value in the app_settings table.

        args:
            key: the setting key.
            value: the setting value.

        returns:
            bool: true if successful, false otherwise.
        """

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO app_settings (key, value) VALUES (?, ?)",
                (key, value)
            )
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"error saving setting {key}: {e}")
            return False

    def add_restaurant(self, restaurant_id: str, name: str, address: str,
                      hours: Union[Dict[str, str], str], best_taco: str) -> bool:
        """
        add a new restaurant to the database.

        args:
            restaurant_id: the yelp business id.
            name: restaurant name.
            address: restaurant address.
            hours: business hours as dict or json string.
            best_taco: the best taco at this restaurant.

        returns:
            bool: true if successful, false otherwise.
        """

        # convert hours dict to json string if needed
        if isinstance(hours, dict):
            hours = json.dumps(hours)

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute('''
            INSERT OR REPLACE INTO taco_restaurants (id, name, address, hours, best_taco)
            VALUES (?, ?, ?, ?, ?)
            ''', (restaurant_id, name, address, hours, best_taco))

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"error adding restaurant: {e}")
            return False

    def add_review(self, restaurant_id: str, text: str, rating: Optional[float] = None,
                  date: Optional[str] = None) -> bool:
        """
        add a new review to the database.

        args:
            restaurant_id: the yelp business id.
            text: review text.
            rating: review rating (optional).
            date: review date (optional).

        returns:
            bool: true if successful, false otherwise.
        """

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute('''
            INSERT OR IGNORE INTO reviews (restaurant_id, text, rating, date)
            VALUES (?, ?, ?, ?)
            ''', (restaurant_id, text, rating, date))

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"error adding review: {e}")
            return False

    def delete_restaurant(self, restaurant_id: str) -> bool:
        """
        delete a restaurant and its reviews from the database.

        args:
            restaurant_id: the yelp business id.

        returns:
            bool: true if successful, false otherwise.
        """

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # delete reviews first (foreign key constraint)
            cursor.execute("DELETE FROM reviews WHERE restaurant_id = ?", (restaurant_id,))

            # then delete the restaurant
            cursor.execute("DELETE FROM taco_restaurants WHERE id = ?", (restaurant_id,))

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"error deleting restaurant: {e}")
            return False

    def get_restaurants_by_zip(self) -> Dict[str, int]:
        """
        get count of restaurants by zip code.

        returns:
            dict: mapping of zip codes to restaurant counts.
        """

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("SELECT address FROM taco_restaurants")
            addresses = cursor.fetchall()
            conn.close()

            zip_counts = {}
            for (address,) in addresses:
                if address:
                    # try to extract zip code from address
                    parts = address.split()
                    for part in parts:
                        # look for 5-digit zip codes
                        if part.isdigit() and len(part) == 5:
                            zip_code = part
                            zip_counts[zip_code] = zip_counts.get(zip_code, 0) + 1
                            break

            return zip_counts
        except Exception as e:
            print(f"error getting restaurants by zip: {e}")
            return {}


# convenience functions for direct use without instantiating the class

def get_all_restaurants() -> List[Dict[str, Any]]:
    """
    get all restaurants from the database.

    returns:
        list: a list of dictionaries containing restaurant data.
    """

    db = TacoDB()
    return db.get_all_restaurants()

def get_restaurant_by_id(restaurant_id: str) -> Optional[Dict[str, Any]]:
    """
    get a specific restaurant by its id.

    args:
        restaurant_id: the yelp business id of the restaurant.

    returns:
        dict: restaurant data or none if not found.
    """

    db = TacoDB()
    return db.get_restaurant_by_id(restaurant_id)

def get_reviews_for_restaurant(restaurant_id: str) -> List[Dict[str, Any]]:
    """
    get all reviews for a specific restaurant.

    args:
        restaurant_id: the yelp business id of the restaurant.

    returns:
        list: a list of dictionaries containing review data.
    """

    db = TacoDB()
    return db.get_reviews_for_restaurant(restaurant_id)

def search_restaurants(query: str) -> List[Dict[str, Any]]:
    """
    search for restaurants by name or address.

    args:
        query: the search query string.

    returns:
        list: a list of dictionaries containing matching restaurant data.
    """

    db = TacoDB()
    return db.search_restaurants(query)

def get_restaurants_by_taco(taco_type: str) -> List[Dict[str, Any]]:
    """
    get restaurants that have a specific best taco.

    args:
        taco_type: the type of taco to search for.

    returns:
        list: a list of dictionaries containing matching restaurant data.
    """

    db = TacoDB()
    return db.get_restaurants_by_taco(taco_type)

def get_popular_tacos(limit: int = 10) -> List[Tuple[str, int]]:
    """
    get the most popular taco types based on frequency.

    args:
        limit: maximum number of results to return.

    returns:
        list: a list of tuples containing (taco_type, count).
    """

    db = TacoDB()
    return db.get_popular_tacos(limit)