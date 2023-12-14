import ydb
import pandas as pd
import os
import random
from bs4 import BeautifulSoup
import requests
import re

ani_dataset = os.getenv("ani_dataset")

driver_config = ydb.DriverConfig(
    endpoint=os.getenv("YDB_ENDPOINT"),
    database=os.getenv("YDB_DATABASE"),
    credentials=ydb.iam.MetadataUrlCredentials()
)

driver = ydb.Driver(driver_config)
# Wait for the driver to become active for requests.
driver.wait(fail_fast=True, timeout=5)
# Create the session pool instance to manage YDB sessions.
pool = ydb.SessionPool(driver)

def upsert_register(uid, login, password):
    text = f"UPSERT INTO myusers (`id`, `login`, `password`) VALUES ( '{uid}', '{login}', '{password}') ;"
    return pool.retry_operation_sync(lambda s: s.transaction().execute(
        text,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))

def return_uID(login, password):
    text = f"SELECT * FROM myusers WHERE `login` == '{login}' AND `password` == '{password}';"
    data = pool.retry_operation_sync(lambda s: s.transaction().execute(
        text,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))
    data_new = pd.DataFrame.from_records(data[0].rows)
    return data_new

def register(response, method):
    if method == "POST":
        try:
            login = response['multiValueParams']['login'][0]
            password = response['multiValueParams']['password'][0]
            exist_user = return_uID(login, password)
            if exist_user.empty:
                uid = str(random.randint(1, 10000))+login
                upsert_register(uid, login, password)
                result = '<div id = "SecretKey">Your secret key is '+ uid + ' , remember it.</div>'
            else:
                result = "Login is already taken, try different one"
        except:
            result = "Something is wrong with login or password"
    else:
        result = 'Method ' + method + ' is not supported'
    return result

def remind(response, method):
    if method == "GET":
        try:
            login = response['multiValueParams']['login'][0]
            password = response['multiValueParams']['password'][0]
            user_data = return_uID(login, password)
            if user_data.empty:
                result = "No such combination"
            else:
                uid = user_data.loc[0]['id']
                result = '<div id = "SecretKey">Your secret key is '+ uid + ' , remember it.</div>'
        except:
            result = "Something is wrong with login or password"
    else:
        result = 'Method ' + method + ' is not supported'
    return result

def parse_search(response):
    s_query = response['multiValueParams']['s_query'][0]
    url = f"https://myanimelist.net/anime.php?q={s_query}&cat=anime"
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    soup_find = soup.find_all("tr")
    url_list = soup_find[9:14]
    result = ''
    for elem in url_list:
        anipage_url = elem.find('a', class_='hoverinfo_trigger fw-b fl-l').attrs['href']
        anipage = requests.get(anipage_url)
        soup_anipage = BeautifulSoup(anipage.content, "html.parser")
        try:
            name_english = soup_anipage.find('p', class_='title-english title-inherit').text
        except:
            name_english = soup_anipage.find('h1', class_='title-name').text
        name_japan = soup_anipage.find('h1', class_='title-name h1_bold_none').text
        score = soup_anipage.find('div', class_='score-label').text
        popularity = soup_anipage.find('span', class_='numbers popularity').text
        anime_id = anipage_url.split('anime/')[1].split('/')[0]
        ranked = soup_anipage.find('span', class_='numbers ranked').text
        result = result + '<div class = "anime_search"><h3>'+name_english+'</h3>'+ '<p>'+name_japan+'</p>' + '<p> Average score: '+score+'</p>' +  '<p>'+popularity+'</p>' + '<p>'+ranked+'</p>' +'<p> Anime id: '+anime_id+'</p>' +'<p>More info: '+f"<a href='{anipage_url}'>MyAnimeList</a>"+'</p></div>'
    return result


def check_user (uID):
    text = f"SELECT * FROM myusers WHERE `id` == '{uID}';"
    data = pool.retry_operation_sync(lambda s: s.transaction().execute(
        text,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))
    data_new = pd.DataFrame.from_records(data[0].rows)
    return data_new

def upsert_wishlist(uID, animeID):
    n_id = uID +"_"+animeID
    text = f"UPSERT INTO wishlist (`id`, `uID`, `animeID`) VALUES ( '{n_id}', '{uID}', '{animeID}') ;"
    return pool.retry_operation_sync(lambda s: s.transaction().execute(
        text,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))

def upsert_watched(uID, animeID, animeRate):
    n_id = uID +"_"+animeID
    text = f"UPSERT INTO watchedlist (`id`, `uID`, `animeID`, `animeRate`) VALUES ( '{n_id}', '{uID}', '{animeID}', '{animeRate}') ;"
    return pool.retry_operation_sync(lambda s: s.transaction().execute(
        text,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))

def upsert_animebase(animeID, eng_name, score, premiered):
    text = f"UPSERT INTO animebase (`id`, `name`, `premiered`, `score`) VALUES ( '{animeID}', '{eng_name}', '{premiered}', '{score}') ;"
    return pool.retry_operation_sync(lambda s: s.transaction().execute(
        text,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))

def add_to_wishlist(response, method):
    if method == "POST":
        try:
            uID = response['multiValueParams']['uID'][0]
            animeID = response['multiValueParams']['animeID'][0]
            user_data = check_user(uID)
            if user_data.empty:
                result = "You are not registered yet or there is a mistake in your secret key. Register or check the key."
            else:
                try:
                    URL = f"https://myanimelist.net/anime/{animeID}"
                    page = requests.get(URL)
                    if page.status_code == 404:
                        result = "There is no anime with this ID. Check it and try again."
                    else:
                        upsert_wishlist(uID, animeID)
                        soup = BeautifulSoup(page.content, "html.parser")
                        try:
                            eng_name = soup.find('p', class_='title-english title-inherit').text
                            eng_name = eng_name.replace("'", '')
                        except:
                            eng_name = soup.find('h1', class_='title-name').text
                            eng_name = eng_name.replace("'", '')
                        anime_info = soup.find('div', class_="leftside")
                        score = soup.find('div', class_='score-label').text
                        text = anime_info.contents
                        text = str(text)
                        try:
                            end = re.search("Premiered", text).end()
                            around_prem = text[end:end + 100].split('\n')[1]
                            premiered = re.search(r'>.*<', around_prem).group()[1:-1]
                        except:
                            premiered = "Null"
                        upsert_animebase(animeID, eng_name, score, premiered)
                        if premiered == "Null":
                            result = "You have added the anime " + eng_name + " with rating " + score +" to your wishlist!"
                        else:
                            result = "You have added the anime " + eng_name + " (premiered in "+premiered +") with rating " + score +" to your wishlist!"

                except:
                    result = ("Something is wrong with MyAnimeList now. We will add this anime ID into your list without "
                            "checking it.")
                    upsert_wishlist(uID, animeID)
        except:
            result = "Something is wrong with your secret key or anime ID"
    else:
        result = 'Method ' + method + ' is not supported'
    return result


def get_wishlist (uID):
    text = f"SELECT animeID FROM wishlist WHERE `uID` == '{uID}';"
    data = pool.retry_operation_sync(lambda s: s.transaction().execute(
        text,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))
    data_new = pd.DataFrame.from_records(data[0].rows)
    return data_new

def get_watchlist (uID):
    text = f"SELECT animeID, animeRate FROM watchedlist WHERE `uID` == '{uID}';"
    data = pool.retry_operation_sync(lambda s: s.transaction().execute(
        text,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))
    data_new = pd.DataFrame.from_records(data[0].rows)
    return data_new

def get_animebase ():
    text = f"SELECT * FROM animebase;"
    data = pool.retry_operation_sync(lambda s: s.transaction().execute(
        text,
        commit_tx=True,
        settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
    ))
    data_new = pd.DataFrame.from_records(data[0].rows)
    return data_new

def return_user_watchlist(response, method):
    if method == "GET":
        try:
            uID = response['multiValueParams']['uID'][0]
            user_data = check_user(uID)
            if user_data.empty:
                result = "You are not registered yet or there is a mistake in your secret key. Register or check the key."
            else:
                animeList = get_watchlist(uID)
                animebase = get_animebase ()
                data_full = animeList.merge(animebase, left_on='animeID', right_on='id', how='left').drop('id', axis=1)
                result = ''
                for i in range(len(data_full)):
                    animeID = data_full.loc[i].animeID
                    animeRate = data_full.loc[i].animeRate
                    name = data_full.loc[i]['name']
                    if str(name) == 'Null' or str(name) == 'nan':
                        name = "No info"
                    else:
                        pass
                    premiered = data_full.loc[i].premiered
                    if str(premiered) == 'Null' or str(premiered) == 'nan':
                        premiered = "No info"
                    else:
                        pass
                    score = data_full.loc[i].score
                    if str(score) == 'Null' or str(score) == 'nan':
                        score = "No info"
                    else:
                        pass
                    result = (result +
                            f"""<div class = 'watchItem'>
    <p> ID: {animeID} </p>
    <h3> Name: {name} </h3>
    <p> Premiered: {premiered} </p>
    <p> Score: {score} </p>
    <p> You have rated: {animeRate} </p>
    <button class ='delWatch' id = '{animeID}' onclick='delFromWatch(this.id)'>Delete</button>
</div>
""")
        except:
            result = "Something is wrong with your secret key"
    else:
        result = 'Method ' + method + ' is not supported'
    return result


def return_user_wishlist(response, method):
    if method == "GET":
        try:
            uID = response['multiValueParams']['uID'][0]
            user_data = check_user(uID)
            if user_data.empty:
                result = "You are not registered yet or there is a mistake in your secret key. Register or check the key."
            else:
                animeList = get_wishlist(uID)
                animebase = get_animebase ()
                data_full = animeList.merge(animebase, left_on='animeID', right_on='id', how='left').drop('id', axis=1)
                result = ''
                for i in range(len(data_full)):
                    animeID = data_full.loc[i].animeID
                    name = data_full.loc[i]['name']
                    if str(name) == 'Null' or str(name) == 'nan':
                        name = "No info"
                    else:
                        pass
                    premiered = data_full.loc[i].premiered
                    if str(premiered) == 'Null' or str(premiered) == 'nan':
                        premiered = "No info"
                    else:
                        pass
                    score = data_full.loc[i].score
                    if str(score) == 'Null' or str(score) == 'nan':
                        score = "No info"
                    else:
                        pass
                    result = (result +
                            f"""<div class = 'wishItem'>
    <p> ID: {animeID} </p>
    <h3> Name: {name} </h3>
    <p> Premiered: {premiered} </p>
    <p> Score: {score} </p>
    <button class ='delWish' id = '{animeID}' onclick='delFromWish(this.id)'>Delete</button>
    <button class ='openMoveToWach' id = '{animeID}' onclick='openMoveWatched(this.id)'>Move to Watched List</button>
    <div id = "{animeID}_rateForm" style="display: none;">
        <p> To move this anime to Wached list enter your rate from 1 to 10 and click Submit </p>
        <input type="text" id="{animeID}_rate">
        <button class ='moveToWatched' id = '{animeID}' onclick='moveWatched(this.id)'>Submit</button>    
    </div>
</div>
""")
        except:
            result = "Something is wrong with your secret key"
    else:
        result = 'Method ' + method + ' is not supported'
    return result

def delete_from_wishlist(response, method):
    if method == "DELETE":
        try:
            wishID = response['multiValueParams']['wishID'][0]
            text = f"DELETE FROM wishlist WHERE `id` == '{wishID}';"
            return pool.retry_operation_sync(lambda s: s.transaction().execute(
                text,
                commit_tx=True,
                settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
            ))
        except:
            result = "Something went wrong..."
    else:
        result = 'Method ' + method + ' is not supported'
    return result

def delete_from_watchedlist(response, method):
    if method == "DELETE":
        try:
            watchID = response['multiValueParams']['watchID'][0]
            text = f"DELETE FROM watchedlist WHERE `id` == '{watchID}';"
            return pool.retry_operation_sync(lambda s: s.transaction().execute(
                text,
                commit_tx=True,
                settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
            ))
        except:
            result = "Something went wrong..."
    else:
        result = 'Method ' + method + ' is not supported'
    return result

def add_to_watched(response, method):
    if method == "POST":
        try:
            uID = response['multiValueParams']['uID'][0]
            animeID = response['multiValueParams']['animeID'][0]
            animeRate = response['multiValueParams']['animeRate'][0]
            user_data = check_user(uID)
            if user_data.empty:
                result = "You are not registered yet or there is a mistake in your secret key. Register or check the key."
            else:
                try:
                    URL = f"https://myanimelist.net/anime/{animeID}"
                    page = requests.get(URL)
                    if page.status_code == 404:
                        result = "There is no anime with this ID. Check it and try again."
                    else:
                        upsert_watched(uID, animeID, animeRate)
                        soup = BeautifulSoup(page.content, "html.parser")
                        try:
                            eng_name = soup.find('p', class_='title-english title-inherit').text
                            eng_name = eng_name.replace("'", '')
                        except:
                            eng_name = soup.find('h1', class_='title-name').text
                            eng_name = eng_name.replace("'", '')
                        anime_info = soup.find('div', class_="leftside")
                        score = soup.find('div', class_='score-label').text
                        text = anime_info.contents
                        text = str(text)
                        try:
                            end = re.search("Premiered", text).end()
                            around_prem = text[end:end + 100].split('\n')[1]
                            premiered = re.search(r'>.*<', around_prem).group()[1:-1]
                        except:
                            premiered = "Null"
                        upsert_animebase(animeID, eng_name, score, premiered)
                        if premiered == "Null":
                            result = "You have added the anime " + eng_name + " with rating " + score +" to list of watched anime! Your rate for this title is " +animeRate+"."
                        else:
                            result = "You have added the anime " + eng_name + " (premiered in "+premiered +") with rating " + score +" to list of watched anime! Your rate for this title is " +animeRate+"."

                except:
                    result = ("Something is wrong with MyAnimeList now. We will add this anime ID into your watched list without "
                            "checking it. Your rate for this title is " +animeRate+".")
                    upsert_watched(uID, animeID, animeRate)
        except:
            result = "Something is wrong with your secret key or anime ID"
    else:
        result = 'Method ' + method + ' is not supported'
    return result


def random_recom(response, method):
    if method == "GET":
        try:
            uID = response['multiValueParams']['uID'][0]
            anime_top = pd.read_csv(f"{ani_dataset}", encoding="unicode_escape")
            if str(uID) == 'null':
                anime_recom = anime_top.sample(10)
            else:
                animeList = get_watchlist(uID)
                anime_notwatched = anime_top.loc[~anime_top['anime_id'].isin(animeList['animeID'])]
                anime_recom = anime_notwatched.sample(10)
            result = ''
            for i in anime_recom.index:
                animeID = anime_recom.loc[i].anime_id
                name = anime_recom.loc[i]['name']
                genres = anime_recom.loc[i].genre
                anime_type = anime_recom.loc[i]['type']
                community = anime_recom.loc[i].members
                rating = anime_recom.loc[i].rating
                episodes = anime_recom.loc[i].episodes
                result = (result +
                        f"""<div class = 'recomItem'>
    <p> ID: {animeID} </p>
    <p> Name: {name} </p>
    <p> Genres: {genres} </p>
    <p> Score: {rating} </p>
    <p> Type: {anime_type} </p>
    <p> Number of episodes: {episodes} </p>
    <p> Size of community: {community} </p>
</div>
""")
        except:
            anime_top = pd.read_csv(f"{ani_dataset}", encoding="unicode_escape")
            anime_recom = anime_top.sample(10)
            result = ''
            for i in anime_recom.index:
                animeID = anime_recom.loc[i].anime_id
                name = anime_recom.loc[i]['name']
                genres = anime_recom.loc[i].genre
                anime_type = anime_recom.loc[i]['type']
                community = anime_recom.loc[i].members
                rating = anime_recom.loc[i].rating
                episodes = anime_recom.loc[i].episodes
                result = (result +
                        f"""<div class = 'recomItem'>
    <p> ID: {animeID} </p>
    <p> Name: {name} </p>
    <p> Genres: {genres} </p>
    <p> Score: {rating} </p>
    <p> Type: {anime_type} </p>
    <p> Number of episodes: {episodes} </p>
    <p> Size of community: {community} </p>
</div>
""")
    else:
        result = 'Method ' + method + ' is not supported'
    return result