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
        conn = sqlite3.connect(db_path)
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
        conn = sqlite3.connect(db_path)
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
        conn = sqlite3.connect(db_path)
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

def parse_arguments():
    """
    parse command line arguments.
    returns the parsed arguments object.
    """
    parser = argparse.ArgumentParser(description="view taco restaurants database")
    parser.add_argument("--id", help="view specific restaurant by id")
    return parser.parse_args()

def main():
    args = parse_arguments()

    if args.id:
        view_restaurant_by_id(DB_PATH, args.id)
    else:
        view_all_restaurants(DB_PATH)

if __name__ == "__main__":
    main()