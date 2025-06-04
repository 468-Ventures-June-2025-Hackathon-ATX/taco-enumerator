import os
import time
import requests
import pandas as pd
import argparse
import logging
import json
import sqlite3
from openai import OpenAI

# ─── CONFIGURATION ────────────────────────────────────────────────────────────
# 0) llm provider toggle
USE_OPENROUTER = True  # set to false to use openai directly

# 1) yelp fusion api
YELP_API_KEY = os.getenv("YELP_API_KEY")
YELP_SEARCH_ENDPOINT = "https://api.yelp.com/v3/businesses/search"
YELP_BUSINESS_ENDPOINT = "https://api.yelp.com/v3/businesses/"

# 2) openai api
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# 3) openrouter api
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "sk-or-v1-...")
OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"

# 3) sqlite database where we store processed taco restaurants
DB_PATH = "taco_restaurants.db"

# 4) search parameters
LOCATION = "Austin, TX"
TERM = "taco"
LIMIT_PER_PAGE = 50  # max yelp allows is 50


# ─── HELPERS ────────────────────────────────────────────────────────────────────
def init_database(db_path: str):
    """
    initialize the sqlite database if it doesn't exist.
    creates the taco_restaurants table and reviews table with required columns.
    """
    conn = sqlite3.connect(db_path)
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

    if DEBUG:
        logging.debug(f"database initialized at: {db_path}")


def get_setting(db_path: str, key: str, default_value: str = "") -> str:
    """
    get a setting value from the app_settings table.
    returns the default value if the key doesn't exist.
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM app_settings WHERE key = ?", (key,))
        result = cursor.fetchone()
        conn.close()

        if result:
            return result[0]
        return default_value
    except Exception as e:
        if DEBUG:
            logging.debug(f"error getting setting {key}: {e}")
        return default_value


def set_setting(db_path: str, key: str, value: str):
    """
    store a setting value in the app_settings table.
    uses insert or replace to handle both new and existing keys.
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO app_settings (key, value) VALUES (?, ?)",
            (key, value)
        )
        conn.commit()
        conn.close()

        if DEBUG:
            logging.debug(f"saved setting {key}={value}")
    except Exception as e:
        if DEBUG:
            logging.debug(f"error saving setting {key}: {e}")


def get_last_offset(db_path: str) -> int:
    """
    get the last used offset from the database.
    returns 0 if not set.
    """
    try:
        offset_str = get_setting(db_path, "last_offset", "0")
        return int(offset_str)
    except ValueError:
        return 0


def save_last_offset(db_path: str, offset: int):
    """
    save the current offset to the database for next run.
    """
    set_setting(db_path, "last_offset", str(offset))


def load_existing_restaurants(db_path: str) -> set:
    """
    load existing restaurant ids (yelp business ids) from database.
    returns a set of business ids (strings).
    if the database does not exist, returns an empty set.
    handles potential errors when reading from the database.
    """
    if not os.path.isfile(db_path):
        init_database(db_path)
        return set()

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM taco_restaurants")
        ids = cursor.fetchall()
        conn.close()

        # convert list of tuples to set of strings
        return set(id_tuple[0] for id_tuple in ids)
    except Exception as e:
        print(f"error reading database {db_path}: {e}")
        return set()


def insert_restaurant(db_path: str, restaurant: tuple):
    """
    insert a restaurant record into the database.
    restaurant is a tuple of (id, name, address, hours, best_taco).
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute('''
        INSERT INTO taco_restaurants (id, name, address, hours, best_taco)
        VALUES (?, ?, ?, ?, ?)
        ''', restaurant)

        conn.commit()
        conn.close()
    except Exception as e:
        print(f"error inserting restaurant into database: {e}")


def insert_reviews(db_path: str, restaurant_id: str, reviews: list):
    """
    insert multiple review records into the database.
    reviews is a list of review objects from the yelp api.
    uses insert or ignore to prevent duplicate reviews.
    """
    if not reviews:
        return

    try:
        # first, let's debug what we're getting
        if DEBUG:
            logging.debug(f"inserting {len(reviews)} reviews for restaurant {restaurant_id}")
            if reviews and isinstance(reviews[0], dict):
                logging.debug(f"review sample: {list(reviews[0].keys())}")
                logging.debug(f"first review: {reviews[0]}")
            else:
                logging.debug(f"review type: {type(reviews[0])}")

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # check if we need to migrate the reviews table to add unique constraint
        has_unique_constraint = False

        # check if the table has a unique index on text
        cursor.execute("PRAGMA index_list(reviews)")
        indexes = cursor.fetchall()

        # simplified check for unique constraint on text column
        for idx in indexes:
            index_name = idx[1]
            if 'text' in index_name or index_name == 'idx_reviews_text':
                has_unique_constraint = True
                break

        # if no unique constraint exists, create one
        if not has_unique_constraint:
            if DEBUG:
                logging.debug("adding unique constraint to reviews table")
            try:
                # create a unique index on text only
                cursor.execute('''
                CREATE UNIQUE INDEX IF NOT EXISTS idx_reviews_text
                ON reviews(text)
                ''')
                conn.commit()
            except Exception as e:
                if DEBUG:
                    logging.debug(f"error creating unique index: {e}")

        # insert reviews using INSERT OR IGNORE to skip duplicates
        inserted_count = 0
        for i, review in enumerate(reviews):
            try:
                # ensure review is a dictionary
                if not isinstance(review, dict):
                    if DEBUG:
                        logging.debug(f"skipping non-dict review at index {i}: {type(review)}")
                    continue

                # extract fields with safe fallbacks
                text = review.get("text", "")
                if not text:  # skip empty reviews
                    continue

                rating = review.get("rating")
                if rating is not None:
                    try:
                        rating = float(rating)
                    except (ValueError, TypeError):
                        rating = None

                time_created = review.get("time_created")

                # insert with proper error handling
                cursor.execute('''
                INSERT OR IGNORE INTO reviews (restaurant_id, text, rating, date)
                VALUES (?, ?, ?, ?)
                ''', (restaurant_id, text, rating, time_created))

                if cursor.rowcount > 0:
                    inserted_count += 1
            except Exception as e:
                if DEBUG:
                    logging.debug(f"error inserting review at index {i}: {e}")
                    if isinstance(review, dict):
                        logging.debug(f"review keys: {list(review.keys())}")
                        for key, value in review.items():
                            logging.debug(f"  {key}: {type(value)}")
                    else:
                        logging.debug(f"review type: {type(review)}")

        conn.commit()
        conn.close()

        if DEBUG:
            logging.debug(f"inserted {inserted_count} reviews for restaurant {restaurant_id}")
    except Exception as e:
        print(f"error inserting reviews into database: {e}")
        if DEBUG:
            import traceback
            logging.debug(f"full error inserting reviews: {e}")
            logging.debug(f"traceback: {traceback.format_exc()}")


def yelp_search_taco_restaurants(location: str, term: str, offset: int = 0) -> dict:
    """
    call yelp fusion search api to find taco restaurants.
    returns the json response.
    handles 400 errors which may occur when offset exceeds api limits.
    """
    headers = {"Authorization": f"Bearer {YELP_API_KEY}"}
    params = {
        "term": term,
        "location": location,
        "limit": LIMIT_PER_PAGE,
        "offset": offset,
        "categories": "tacos",  # restrict to taco category
    }

    if DEBUG:
        logging.debug(f"calling yelp search api with params: {params}")

    try:
        response = requests.get(YELP_SEARCH_ENDPOINT, headers=headers, params=params)
        response.raise_for_status()
        result = response.json()

        if DEBUG:
            total_results = result.get("total", 0)
            businesses_count = len(result.get("businesses", []))
            logging.debug(f"yelp search returned {businesses_count} businesses (total available: {total_results})")

        return result
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 400:
            if DEBUG:
                logging.debug(f"yelp api rejected request with offset={offset}, likely exceeded maximum allowed offset")
            # return empty result to signal end of pagination
            return {"businesses": [], "total": 0}
        # re-raise other http errors
        raise


def yelp_get_business_details(business_id: str) -> dict:
    """
    call yelp fusion business api to fetch detailed info (including hours).
    """
    headers = {"Authorization": f"Bearer {YELP_API_KEY}"}
    url = f"{YELP_BUSINESS_ENDPOINT}{business_id}"

    if DEBUG:
        logging.debug(f"fetching business details for id: {business_id}")

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    result = response.json()

    if DEBUG:
        hours_info = result.get("hours", [])
        has_hours = len(hours_info) > 0
        logging.debug(f"business details for {business_id}: name={result.get('name')}, has_hours={has_hours}")

    return result


def parse_hours_to_json(hours_str: str) -> str:
    """
    parse hours string in format "day hh:mm-hh:mm; day hh:mm-hh:mm" to json.
    returns a json string with days as keys and time ranges as values.
    days without hours will have null value.
    """
    if hours_str == "N/A":
        return json.dumps({day: None for day in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]})

    # initialize all days with null
    hours_dict = {day: None for day in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]}

    # parse each day's hours
    day_hours = hours_str.split("; ")
    for entry in day_hours:
        parts = entry.split(" ")
        if len(parts) == 2:
            day = parts[0]
            time_range = parts[1]
            hours_dict[day] = time_range

    return json.dumps(hours_dict)


def extract_hours_from_yelp(business_json: dict) -> str:
    """
    given a business detail json, extract the hours in json format.
    keys are days of week, values are time ranges or null if closed.
    fallback to json with all null values if hours not available.
    """
    hours_info = business_json.get("hours", [])
    if not hours_info:
        return json.dumps({day: None for day in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]})

    # usually hours_info is a list with one element containing 'open' array
    open_sections = hours_info[0].get("open", [])
    day_map = {
        0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"
    }

    # initialize all days with null
    hours_dict = {day: None for day in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]}

    # populate hours for each day
    for entry in open_sections:
        day = day_map.get(entry.get("day"), "Unknown")
        start = entry.get("start")  # e.g. "1100"
        end = entry.get("end")      # e.g. "2200"
        # convert "1100" → "11:00", etc.
        formatted_start = f"{start[:2]}:{start[2:]}"
        formatted_end = f"{end[:2]}:{end[2:]}"
        hours_dict[day] = f"{formatted_start}-{formatted_end}"

    return json.dumps(hours_dict)


def query_openrouter(prompt: str) -> str:
    """
    query the openrouter api with the given prompt.
    returns the generated text response.
    handles errors and returns "unknown" on failure.
    """
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/yelp-taco-enumerator"
    }

    payload = {
        "model": "anthropic/claude-3-haiku",  # using a fast, cost-effective model
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.7,
        "max_tokens": 50
    }

    try:
        response = requests.post(
            OPENROUTER_API_URL,
            headers=headers,
            data=json.dumps(payload)
        )
        response.raise_for_status()
        result = response.json()
        text = result["choices"][0]["message"]["content"].strip()
        return text.strip().strip('"').strip("'")
    except Exception as e:
        if DEBUG:
            logging.debug(f"openrouter request failed: {e}")
        return "Unknown"


def query_best_taco(restaurant_name: str, reviews_snippet: str) -> str:
    """
    ask an llm to determine the "best taco" based on provided review snippets.
    we send a prompt with the restaurant name and a few reviews; the model returns a short string.
    if it cannot determine, return "unknown".
    uses either openrouter or openai based on the global toggle.
    """
    prompt = (
        f"You are a food critic AI. Given the restaurant '{restaurant_name}' and the following "
        f"recent Yelp review snippets about their tacos:\n\n"
        f"{reviews_snippet}\n\n"
        "Which specific taco item seems to be the most highly praised? "
        "Answer with the taco name only (e.g., \"Al Pastor Taco\"). "
        "If you cannot tell from these snippets, just say 'Unknown'."
        "Your response should be only the name of the taco, without any additional text."
        "Your response should be short, not a complete sentence, just the name of the taco."
    )

    if DEBUG:
        logging.debug(f"prompt for restaurant '{restaurant_name}':")
        logging.debug(f"---prompt start---")
        logging.debug(f"{prompt}")
        logging.debug(f"---prompt end---")

    if USE_OPENROUTER:
        if DEBUG:
            logging.debug(f"using openrouter for restaurant: {restaurant_name}")
        return query_openrouter(prompt)
    else:
        if DEBUG:
            logging.debug(f"using openai for restaurant: {restaurant_name}")
        try:
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=50,
            )
            text = resp.choices[0].message.content.strip()
            # sometimes the model will preface with quotes; strip them.
            return text.strip().strip('"').strip("'")
        except Exception as e:
            print(f"llm request failed for {restaurant_name}: {e}")
            return "Unknown"


def get_top_review_snippets(business_id: str, limit: int = 3) -> tuple:
    """
    yelp fusion reviews endpoint: fetch reviews for the given business_id.
    returns a tuple of (review_objects, concatenated_text).
    if reviews are unavailable or error occurs, return ([], "").

    note: the yelp api deliberately truncates review text with "..." - this is a
    limitation of the api and cannot be bypassed without violating yelp's terms of service.

    note: yelp api seems to return the same reviews regardless of offset, so we only fetch the first batch.
    """
    try:
        headers = {"Authorization": f"Bearer {YELP_API_KEY}"}
        url = f"{YELP_BUSINESS_ENDPOINT}{business_id}/reviews"

        # just get the first batch of reviews (max 3)
        all_reviews = []

        if DEBUG:
            logging.debug(f"fetching up to {limit} reviews for business id: {business_id}")

        # try to get full review text
        # use the best parameters to get maximum review text (though still truncated by Yelp)
        params = {
            "limit": limit,
            "offset": 0,
            "text_format": "original",  # request original text format
            "sort_by": "relevance"      # get most relevant reviews first
        }

        if DEBUG:
            logging.debug(f"fetching reviews batch: offset=0, limit={limit}")

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        reviews = data.get("reviews", [])

        # add reviews to our collection
        all_reviews.extend(reviews)

        # avoid hitting rate limits
        time.sleep(0.2)

        if DEBUG:
            review_count = len(all_reviews)
            avg_length = sum(len(rev.get("text", "")) for rev in all_reviews) // max(1, review_count)
            logging.debug(f"received total of {review_count} reviews for {business_id}, avg length: {avg_length} chars")

        # Note: Yelp API deliberately truncates review text with "..."
        # This is a limitation of the API and cannot be bypassed
        if DEBUG and any(rev.get("text", "").endswith("...") for rev in all_reviews):
            logging.debug("Some reviews are truncated due to Yelp API limitations")

        snippets = [rev.get("text", "") for rev in all_reviews]
        joined_snippets = "\n\n".join(snippets)

        if DEBUG:
            logging.debug(f"review snippets for business {business_id}:")
            logging.debug(f"---snippets start---")
            logging.debug(f"{joined_snippets}")
            logging.debug(f"---snippets end---")

        return all_reviews, joined_snippets
    except Exception as e:
        error_msg = f"failed to fetch reviews for {business_id}: {e}"
        if DEBUG:
            logging.debug(error_msg)
        else:
            print(error_msg)
        return [], ""


def setup_logging(debug_mode: bool):
    """
    configure logging based on debug mode.
    sets up console handler with appropriate format and level.
    """
    global DEBUG
    DEBUG = debug_mode

    if debug_mode:
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [DEBUG] %(message)s',
            datefmt='%H:%M:%S'
        )
        logging.debug("debug mode enabled")
    else:
        logging.basicConfig(
            level=logging.INFO,
            format='%(message)s'
        )


def parse_arguments():
    """
    parse command line arguments.
    returns the parsed arguments object.
    """
    parser = argparse.ArgumentParser(description="yelp taco restaurant finder")
    parser.add_argument("--debug", action="store_true", help="enable debug logging")
    return parser.parse_args()


def get_restaurant_count(db_path: str) -> int:
    """
    get the total count of restaurants in the database.
    returns 0 if the database doesn't exist or an error occurs.
    """
    if not os.path.isfile(db_path):
        return 0

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM taco_restaurants")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except Exception as e:
        print(f"error counting restaurants in database: {e}")
        return 0


# ─── MAIN LOGIC ────────────────────────────────────────────────────────────────
def main():
    # parse command line arguments
    args = parse_arguments()

    # setup logging based on debug flag
    setup_logging(args.debug)

    # 1) ensure database exists and load existing ids
    init_database(DB_PATH)
    existing_ids = load_existing_restaurants(DB_PATH)

    # get the last used offset to start from there
    offset = get_last_offset(DB_PATH)

    if DEBUG:
        logging.debug(f"starting search for '{TERM}' in '{LOCATION}'")
        logging.debug(f"using limit of {LIMIT_PER_PAGE} results per page")
        logging.debug(f"starting from offset: {offset}")

    print(f"found {len(existing_ids)} existing restaurants in the database")
    print(f"starting from offset: {offset}")
    restaurants_processed = 0

    total_api_calls = 0
    while True:
        total_api_calls += 1
        if DEBUG:
            logging.debug(f"search iteration {total_api_calls}, offset: {offset}")

        data = yelp_search_taco_restaurants(LOCATION, TERM, offset=offset)
        businesses = data.get("businesses", [])

        if not businesses:
            if DEBUG:
                logging.debug("no more businesses returned, ending search")
            break

        for biz in businesses:
            biz_id = biz.get("id")
            if not biz_id or biz_id in existing_ids:
                continue  # skip if already processed or no id

            name = biz.get("name", "")
            location = biz.get("location", {})
            address = ", ".join(location.get("display_address", []))

            # 2) fetch business details (hours)
            details = yelp_get_business_details(biz_id)
            hours = extract_hours_from_yelp(details)

            # 3) fetch reviews and ask llm for best taco
            # note: yelp api will truncate review text with "..." - this is their api limitation
            reviews, review_snips = get_top_review_snippets(biz_id, limit=3)
            best_taco = "Unknown"
            if review_snips:
                best_taco = query_best_taco(name, review_snips)

            # store reviews in database
            if reviews:
                try:
                    insert_reviews(DB_PATH, biz_id, reviews)
                except Exception as e:
                    if DEBUG:
                        logging.debug(f"error in main when inserting reviews: {e}")

            # 4) insert this restaurant into database immediately
            restaurant = (biz_id, name, address, hours, best_taco)
            insert_restaurant(DB_PATH, restaurant)
            restaurants_processed += 1
            print(f"→ processed & saved: {name} ({biz_id})")

            # to avoid hitting rate limits—pause briefly
            time.sleep(0.5)

        # yelp allows up to ~1000 results in increments of 50
        offset += LIMIT_PER_PAGE

        # save current offset for next run
        save_last_offset(DB_PATH, offset)

        # if we've reached the end or hit yelp's offset limit (which appears to be ~200), wrap around to 0 for next run
        if offset >= data.get("total", 0) or offset >= 200:
            save_last_offset(DB_PATH, 0)
            break

    # summary of processing
    if restaurants_processed > 0:
        print(f"successfully processed and saved {restaurants_processed} new restaurants to {DB_PATH}")
    else:
        print("no new taco restaurants found.")

    total_count = get_restaurant_count(DB_PATH)
    print(f"total restaurants in database: {total_count}")

    if DEBUG:
        logging.debug(f"script completed: made {total_api_calls} search api calls")
        logging.debug(f"processed {restaurants_processed} new restaurants")


if __name__ == "__main__":
    # before running:
    # • set environment variables: YELP_API_KEY and either OPENAI_API_KEY or OPENROUTER_API_KEY
    # • pip install requests pandas openai>=1.0.0 sqlite3
    main()
