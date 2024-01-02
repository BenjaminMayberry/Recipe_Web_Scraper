import requests
from bs4 import BeautifulSoup
import re
import json
import psycopg2
import time
import random
from passwords import host_name, database_name, user_name, password_name


def allrecipes_data_extraction(soup, link):
    link_elements = soup.find_all("a", class_="comp mntl-card-list-items mntl-document-card mntl-card card card--no-image")
    links = []
    for lin in link_elements:
        links_to_insert = re.findall("https://www.allrecipes.com/.*/", str(lin))
        if links_to_insert != []:
            links.append(links_to_insert)

    if len(links) > 0:
        insert_new_links(links)
    else:
        print(f"No links given on {link}")



def insert_new_links(links):
    try:
        conn = psycopg2.connect(
                host=host_name,
            database=database_name,
            user=user_name,
            password=password_name)
        cur = conn.cursor()
        cur2 = conn.cursor()
        for link in links:
            test = f"select * from recipie_websites where website = '{link[0]}';"
            cur.execute(test)
            if cur.rowcount == 0:
                cur2.execute(f"INSERT INTO recipie_websites (website, visited) VALUES('{link[0]}', false);")
                conn.commit()
        cur2.close()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(link)
        print(f'insert_new_links {error}')
    finally:
        if conn is not None:
            conn.close()
        

def get_links():
    try:
        conn = psycopg2.connect(
                host=host_name,
            database=database_name,
            user=user_name,
            password=password_name)
        cur = conn.cursor()
        cur.execute("select * from recipie_websites where visited = true")
        row = cur.fetchone()
        recipe_links_with_index = []
        while row is not None:
            recipe_links_with_index.append((row[1], row[0]))

            row = cur.fetchone()
            conn.commit()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f'get links {error}')
    finally:
        if conn is not None:
            conn.close()
        return recipe_links_with_index


def update_data(data, key):
    try:
        conn = psycopg2.connect(
                host=host_name,
            database=database_name,
            user=user_name,
            password=password_name)
        cur = conn.cursor()
        data_with_replacement = data.replace("\'", "\'\'")
        # print(data_with_replacement)
        cur.execute(f"update recipie_websites set visited = true, recipie = '{data_with_replacement}' where key = {key}")
        conn.commit()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f'update_data {error}')
    finally:
        if conn is not None:
            conn.close()


def main():
    continue_going = True
    times_though = 0
    items_gone_through = 0
    while continue_going:
        Links = get_links()
        for link in Links:
            page = requests.get(link[0])
            soup = BeautifulSoup(page.content, "html.parser")
            # print(link[0])
            allrecipes_data_extraction(soup, link[0])
            
            time.sleep(random.randint(2, 7))
        if len(Links) <= 0:
            continue_going = False
        # times_though = times_though + 1


if __name__ == '__main__':
    main()