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
        # print(lin)
        links_to_insert = re.findall("href=\"https://www.allrecipes.com/.*\" ", str(lin))
        # print(links_to_insert)
        # Links Insert
        if links_to_insert != []:
            for item in links_to_insert:
                links.append(re.findall('"([^"]*)"', item))
    # print(f"test  {links}")
    if len(links) > 0:
        # print(links)
        for lin in links:
            # print(f"links link link {link}")
            # print(lin)
            if re.fullmatch('https://www.allrecipes.com/article/.*', lin[0]):
                print(f"article link skipped {lin[0]}")
            elif re.fullmatch('https://www.allrecipes.com/.*recipe.*', lin[0]):
                
                if not(re.fullmatch('https://www.allrecipes.com/gallery/.*', lin[0])):
                    # print(re.fullmatch('https://www.allrecipes.com/gallery/.*', lin[0]))
                    insert_new_link(lin[0])
                else:
                    insert_new_link_gallery(lin[0])

            # insert_new_link()
    else:
        print(f"No links given on {link}")
    

def insert_new_link_gallery(link):
    try:
        conn = psycopg2.connect(
                host=host_name,
            database=database_name,
            user=user_name,
            password=password_name)
        cur = conn.cursor()
        cur2 = conn.cursor()
        

        test = f"select * from recipie_websites_gallery where website = '{link}';"
        cur.execute(test)
        if cur.rowcount == 0:
            cur2.execute(f"INSERT INTO recipie_websites_gallery (website, visited) VALUES('{link}', false);")
            conn.commit()
        cur2.close()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(link)
        print(f'insert_new_links {error}')
    finally:
        if conn is not None:
            conn.close()

def insert_new_link(link):
    try:
        conn = psycopg2.connect(
                host=host_name,
            database=database_name,
            user=user_name,
            password=password_name)
        cur = conn.cursor()
        cur2 = conn.cursor()
        test = f"select * from recipie_websites where website = '{link}';"
        cur.execute(test)
        if cur.rowcount == 0:
            cur2.execute(f"INSERT INTO recipie_websites (website, visited) VALUES('{link}', false);")
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
        cur.execute(f"update recipie_websites_test set visited = true, recipie = '{data_with_replacement}' where key = {key}")
        conn.commit()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f'update_data {error}')
    finally:
        if conn is not None:
            conn.close()

def gallery_data_extraction(soup):
    link_elements = soup.find_all("a", class_="mntl-sc-block-featuredlink__link mntl-text-link button--contained-standard type--squirrel")
    # print(link_elements)
    links = []
    for lin in link_elements:
        # print(lin)
        links_to_insert = re.findall("href=\"https://www.allrecipes.com/.*\" ", str(lin))
        # print(links_to_insert)
        # Links Insert
        if links_to_insert != []:
            for item in links_to_insert:
                links.append(re.search('"([^"]*)"', item))
    # print(f"test  {links}")
    if len(links) > 0:
        # print(links)
        for lin in links:
            

            # print(f"links link link {link}")
            curr_link = lin[0].replace("\"","")
            if re.fullmatch('https://www.allrecipes.com/article/.*', curr_link):
                print(f'Artical Link skipped {curr_link}')
            elif re.fullmatch('https://www.allrecipes.com/.*recipe.*', curr_link):

                if not(re.fullmatch('https://www.allrecipes.com/gallery/.*', curr_link)):
                    # print(re.fullmatch('https://www.allrecipes.com/gallery/.*', lin[0]))
                    # print(lin[0].replace("\"",""))
                    insert_new_link(curr_link)
                else:
                    # print(lin[0].replace("\"",""))
                    insert_new_link_gallery(curr_link)


def check_gallery_links(link_key):
    try:
        conn = psycopg2.connect(
                host=host_name,
            database=database_name,
            user=user_name,
            password=password_name)
        cur = conn.cursor()
        for website in link_key:
            # print(website)
            page = requests.get(website[0])
            soup = BeautifulSoup(page.content, "html.parser")
            # print(link[0])
            gallery_data_extraction(soup)
            # print(f"update recipie_websites_gallery set visited = true, website = '{website[0]}' where key = {website[1]}")
            cur.execute(f"update recipie_websites_gallery set visited = true, website = '{website[0]}' where key = {website[1]}")
        conn.commit()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f'update_data {error}')
    finally:
        if conn is not None:
            conn.close()

def gallery_info():
    try:
        conn = psycopg2.connect(
                host=host_name,
            database=database_name,
            user=user_name,
            password=password_name)
        cur = conn.cursor()
        cur.execute("select * from recipie_websites_gallery where visited = false")
        row = cur.fetchone()
        gallery_links_with_index = []
        while row is not None:
            print(row)
            gallery_links_with_index.append((row[1], row[0]))

            row = cur.fetchone()
            conn.commit()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f'get links {error}')
    finally:
        if conn is not None:
            conn.close()
        check_gallery_links(gallery_links_with_index)


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
        continue_going = False
        
        gallery_info()


if __name__ == '__main__':
    main()