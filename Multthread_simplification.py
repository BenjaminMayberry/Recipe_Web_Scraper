import requests
from bs4 import BeautifulSoup
import re
import json
import psycopg2
import multiprocessing as mp
from passwords import host_name, database_name, user_name, password_name

def get_connection():
    return psycopg2.connect(
        host=host_name,
        database=database_name,
        user=user_name,
        password=password_name
    )

def insert_new_link(link, is_gallery=False):
    table_name = "recipie_websites_gallery" if is_gallery else "recipie_websites"
    query = f"INSERT INTO {table_name} (website, visited) VALUES(%s, false);"
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table_name} WHERE website = %s;", (link,))
            if cur.rowcount == 0:
                print(f"Added recipe {link}")
                cur.execute(query, (link,))
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f'Insert new link error: {error}')
    finally:
        if conn is not None:
            conn.close()

def get_links(is_gallery=False):
    table_name = "recipie_websites_gallery" if is_gallery else "recipie_websites"
    query = "SELECT * FROM {} WHERE (visited = false OR re_visit = true);".format(table_name)
    links = []
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            for row in rows:
                links.append(row[1])
                cur.execute("UPDATE {} SET visited = true, re_visit = false WHERE key = %s;".format(table_name), (row[0],))
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f'Get links error: {error}')
    finally:
        if conn is not None:
            conn.close()
    return links

def gallery_data_extraction(link):
    page = requests.get(link)
    soup = BeautifulSoup(page.content, "html.parser")
    link_elements = soup.find_all("a", class_="mntl-sc-block-featuredlink__link mntl-text-link button--contained-standard type--squirrel")
    for lin in link_elements:
        link_match = re.search('href="(https://www.allrecipes.com/[^"]*)"', str(lin))
        if link_match:
            curr_link = link_match.group(1)
            if re.fullmatch('https://www.allrecipes.com/article/.*', curr_link):
                print(f'Article Link skipped: {curr_link}')
            elif re.fullmatch('https://www.allrecipes.com/.*recipe.*', curr_link):
                insert_new_link(curr_link, is_gallery=True)
            elif re.fullmatch('https://www.allrecipes.com/gallery/.*', curr_link):
                insert_new_link(curr_link, is_gallery=True)

def process_link(link):
    page = requests.get(link)
    soup = BeautifulSoup(page.content, "html.parser")
    try:
        ingredients_elements = soup.find_all("li", class_="mntl-structured-ingredients__list-item")
    except Exception as error:
        ingredients_elements = []
    try:
        steps_elements = soup.find("div", class_="comp recipe__steps-content mntl-sc-page mntl-block").find_all("p", class_="comp mntl-sc-block mntl-sc-block-html")
    except Exception as error:
        steps_elements = []
    
    try:
        title = soup.find("h1", class_="comp type--lion article-heading mntl-text-block").text.strip()
    except Exception as error:
        title = "Delete"

    try:
        rate = soup.find("div", class_="comp type--squirrel-bold mntl-recipe-review-bar__rating mntl-text-block").text.strip() if soup.find("div", class_="comp type--squirrel-bold mntl-recipe-review-bar__rating mntl-text-block") else "no rating"
    
    except Exception as error:
        rate = 0

    
    ingredients = [i.text.strip() for i in ingredients_elements]
    steps = [i.text.strip() for i in steps_elements]
    return {
        "title": title,
        "ingredients": ingredients,
        "steps": steps,
        "ben_verified": False,
        "ben_notes": "",
        "rating": rate,
        "source_url": link
    }

def main():
    while True:
        links = get_links()
        if not links:
            break
        with mp.Pool(mp.cpu_count() + 20) as pool:
            pool.map(gallery_data_extraction, links)
        with mp.Pool(mp.cpu_count() + 20) as pool:
            data = pool.map(process_link, links)
            for link, d in zip(links, data):
                json_data = json.dumps(d)
                try:
                    conn = get_connection()
                    with conn.cursor() as cur:
                        cur.execute("UPDATE recipie_websites SET visited = true, re_visit = false, recipie = %s WHERE website = %s;", (json_data, link))
                        conn.commit()
                except (Exception, psycopg2.DatabaseError) as error:
                    print(f'Update data error: {error}')
                finally:
                    if conn is not None:
                        conn.close()

if __name__ == '__main__':
    main()
