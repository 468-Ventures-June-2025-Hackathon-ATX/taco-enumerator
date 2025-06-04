import sqlite3
import argparse
import os
import json

# database path
DB_PATH = "taco_restaurants.db"

def get_reviews_for_restaurant(db_path: str, restaurant_id: str):
    """
    fetch reviews for a specific restaurant.
    returns a list of review tuples (id, text, rating, date).
    """
    if not os.path.isfile(db_path):
        return []

    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        cursor = conn.cursor()

        cursor.execute("""
        SELECT id, text, rating, date
        FROM reviews
        WHERE restaurant_id = ?
        ORDER BY date DESC
        """, (restaurant_id,))

        reviews = cursor.fetchall()
        conn.close()
        return reviews
    except Exception as e:
        print(f"error fetching reviews: {e}")
        return []


def view_all_restaurants(db_path: str):
    """
    display all restaurants in the database.
    """
    if not os.path.isfile(db_path):
        print(f"database file not found: {db_path}")
        return

    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        cursor = conn.cursor()

        # get total count
        cursor.execute("SELECT COUNT(*) FROM taco_restaurants")
        count = cursor.fetchone()[0]
        print(f"total restaurants in database: {count}\n")

        # fetch all restaurants
        cursor.execute("SELECT id, name, address, hours, best_taco FROM taco_restaurants")
        restaurants = cursor.fetchall()

        # display restaurants
        for i, restaurant in enumerate(restaurants, 1):
            biz_id, name, address, hours, best_taco = restaurant
            print(f"restaurant #{i}:")
            print(f"id: {biz_id}")
            print(f"name: {name}")
            print(f"address: {address}")
            # parse and display hours in a readable format
            try:
                hours_dict = json.loads(hours)
                print("hours:")
                for day, time_range in hours_dict.items():
                    status = time_range if time_range else "Closed"
                    print(f"  {day}: {status}")
            except json.JSONDecodeError:
                # fallback for old format
                print(f"hours: {hours}")
            print(f"best taco: {best_taco}")

            # get and display review count
            cursor.execute("SELECT COUNT(*) FROM reviews WHERE restaurant_id = ?", (biz_id,))
            review_count = cursor.fetchone()[0]
            if review_count > 0:
                print(f"reviews: {review_count} (use --id {biz_id} to view)")

            print("-" * 50)

        conn.close()
    except Exception as e:
        print(f"error reading database: {e}")

def view_restaurant_by_id(db_path: str, biz_id: str):
    """
    display a specific restaurant by its id.
    """
    if not os.path.isfile(db_path):
        print(f"database file not found: {db_path}")
        return

    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        cursor = conn.cursor()

        # fetch restaurant
        cursor.execute("""
        SELECT id, name, address, hours, best_taco
        FROM taco_restaurants
        WHERE id = ?
        """, (biz_id,))

        restaurant = cursor.fetchone()

        if restaurant:
            biz_id, name, address, hours, best_taco = restaurant
            print(f"restaurant details:")
            print(f"id: {biz_id}")
            print(f"name: {name}")
            print(f"address: {address}")
            # parse and display hours in a readable format
            try:
                hours_dict = json.loads(hours)
                print("hours:")
                for day, time_range in hours_dict.items():
                    status = time_range if time_range else "Closed"
                    print(f"  {day}: {status}")
            except json.JSONDecodeError:
                # fallback for old format
                print(f"hours: {hours}")
            print(f"best taco: {best_taco}")

            # fetch and display reviews
            reviews = get_reviews_for_restaurant(db_path, biz_id)
            if reviews:
                print(f"\nreviews ({len(reviews)}):")
                for i, review in enumerate(reviews, 1):
                    review_id, text, rating, date = review
                    print(f"  review #{i}:")
                    if rating:
                        print(f"  rating: {rating}/5")
                    if date:
                        print(f"  date: {date}")
                    print(f"  {text}")
                    print()
            else:
                print("\nno reviews found for this restaurant")
        else:
            print(f"no restaurant found with id: {biz_id}")

        conn.close()
    except Exception as e:
        print(f"error reading database: {e}")

def display_stats(db_path: str):
    """
    display statistics about restaurants and reviews in the database.
    shows counts, popular tacos, and zip code distribution.
    """

    if not os.path.isfile(db_path):
        print(f"database file not found: {db_path}")
        return

    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        cursor = conn.cursor()

        # get restaurant count
        cursor.execute("SELECT COUNT(*) FROM taco_restaurants")
        restaurant_count = cursor.fetchone()[0]
        print(f"total restaurants: {restaurant_count}")

        # get review count
        cursor.execute("SELECT COUNT(*) FROM reviews")
        review_count = cursor.fetchone()[0]
        print(f"total reviews: {review_count}")

        # get most popular taco types
        cursor.execute("""
        SELECT best_taco, COUNT(*) as count
        FROM taco_restaurants
        WHERE best_taco IS NOT NULL AND best_taco != ''
        GROUP BY best_taco
        ORDER BY count DESC
        LIMIT 10
        """)
        popular_tacos = cursor.fetchall()

        if popular_tacos:
            print("\nmost popular taco types:")
            for taco, count in popular_tacos:
                print(f"  {taco}: {count} restaurants")

        # get top restaurant chains by number of locations
        cursor.execute("""
        SELECT name, COUNT(*) as count
        FROM taco_restaurants
        GROUP BY name
        HAVING count > 1
        ORDER BY count DESC
        LIMIT 10
        """)
        restaurant_chains = cursor.fetchall()

        if restaurant_chains:
            print("\ntop restaurant chains by locations:")
            for name, count in restaurant_chains:
                print(f"  {name}: {count} locations")

        # extract zip codes and count restaurants per zip
        print("\nrestaurants by zip code:")
        cursor.execute("SELECT address FROM taco_restaurants")
        addresses = cursor.fetchall()

        zip_counts = {}
        for (address,) in addresses:
            if address:
                # try to extract zip code from address
                # assuming format like "123 Main St, City, State 12345" or similar
                parts = address.split()
                for part in parts:
                    # look for 5-digit zip codes
                    if part.isdigit() and len(part) == 5:
                        zip_code = part
                        zip_counts[zip_code] = zip_counts.get(zip_code, 0) + 1
                        break

        # display zip code stats
        if zip_counts:
            # sort by count (descending) and filter out single-restaurant zips
            sorted_zips = sorted(zip_counts.items(), key=lambda x: x[1], reverse=True)
            filtered_zips = [(zip_code, count) for zip_code, count in sorted_zips if count > 1]

            if filtered_zips:
                for zip_code, count in filtered_zips:
                    print(f"  zip {zip_code}: {count} restaurants")
            else:
                print("  no zip codes with multiple restaurants found")
        else:
            print("  no zip codes found in addresses")

        conn.close()
    except Exception as e:
        print(f"error generating stats: {e}")

def parse_arguments():
    """
    parse command line arguments.
    returns the parsed arguments object.
    """
    parser = argparse.ArgumentParser(description="view taco restaurants database")
    parser.add_argument("--id", help="view specific restaurant by id")
    parser.add_argument("--stats", action="store_true", help="display statistics about restaurants and reviews")
    return parser.parse_args()

def main():
    args = parse_arguments()

    if args.stats:
        display_stats(DB_PATH)
    elif args.id:
        view_restaurant_by_id(DB_PATH, args.id)
    else:
        view_all_restaurants(DB_PATH)

if __name__ == "__main__":
    main()