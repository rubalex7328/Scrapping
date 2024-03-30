import requests
from bs4 import BeautifulSoup
import json
from pymongo.mongo_client import MongoClient
import certifi


URL = "https://quotes.toscrape.com"


def get_quotes() -> tuple:
    quotes_lst = []
    authors_dict = {}
    next_link = ''
    while True:
        response = requests.get(URL+next_link)
        soup = BeautifulSoup(response.text, 'html.parser')
        quotes = soup.select('div[class=quote]')
        for q in quotes:
            quotes_dict = {}
            author_dict = {}
            quote = q.find('span', attrs={"class": "text"}).text.strip()
            author = q.find('small', attrs={"class": "author"}).text.strip()
            auth_url = q.find('a').attrs['href'].strip()
            author_dict['author'] = author
            author_dict['url'] = auth_url
            q_tags = q.find('div',attrs={"class": "tags"})
            quote_tags = q_tags.find_all('a',attrs={"class": "tag"})
            tags = []
            for t in quote_tags:
                tags.append(t.text.strip())
            quotes_dict['tags'] = tags
            quotes_dict['author'] = author
            quotes_dict['quote'] = quote
            quotes_lst.append(quotes_dict)
            authors_dict[author] = author_dict
        link = soup.select("li[class=next]")
        if len(link) != 0:
            next_link = link[0].find('a').attrs['href'].strip()
            if next_link is None:
                break
        else:
            break

    return  quotes_lst, authors_dict


def get_authors(authors_url:dict) -> list:
    authors_lst = []
    for author_item in authors_url.values():
        response = requests.get(URL+author_item['url'])
        soup = BeautifulSoup(response.text, 'html.parser')
        author = {'fullname': author_item['author'].strip()}
        born_date = soup.find('span',attrs={"class": "author-born-date"}).text.strip()
        author['born_date'] = born_date
        born_location = soup.find('span',attrs={"class": "author-born-location"}).text.strip()
        author['born_location'] = born_location
        description = soup.find('div',attrs={"class": "author-description"}).text
        author['description'] = description.replace('\n','').strip()
        authors_lst.append(author)
    return authors_lst

def connect_mongo() -> MongoClient:
    """
    connect - makes connect to Atlas Mongo DB
    :return:
        MongoClient
    """
    uri = """mongodb+srv://hwuser:YnBigMcEmZ2yYH4u@cluster0.oaucwy5.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"""

    # Create a new client and connect to the server
    client = MongoClient(uri, tlsCAFile=certifi.where())
    # Send a ping to confirm a successful connectio
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
        return client
    except Exception as e:
        print(e)
        raise e

def save_to_mongo(quotes_file,authors_file):
    mongo_client = connect_mongo()
    db = mongo_client.scrabbing

    with open(quotes_file,'r',encoding='utf-8') as f:
        quotes = json.load(f)
        result = db.quotes.insert_many(quotes)
        print(result)
    print('load_authors')
    with open(authors_file,'r',encoding='utf-8') as f:
        authors = json.load(f)
        result = db.authors.insert_many(authors)
        print(result)


def main():
    quotes, authors_url = get_quotes()
    with open('quotes.json','w',encoding='utf-8') as f:
        json.dump(quotes,f,ensure_ascii=False,indent=4)
    print('get_authors')
    authors = get_authors(authors_url)
    with open('authors.json','w',encoding='utf-8') as f:
        json.dump(authors,f,ensure_ascii=False,indent=4)



if __name__ == '__main__':
    #main()
    save_to_mongo('quotes.json','authors.json')


