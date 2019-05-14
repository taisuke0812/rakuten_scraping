#coding: UTF-8
from datetime import datetime
from datetime import timedelta
from urllib import request
import urllib.parse
from bs4 import BeautifulSoup
import pandas as pd
from pandas.io import gbq
import time
import gc



_time = datetime.now()
_time = _time.strftime("%Y/%m/%d %H:%M")
_time = datetime.strptime(_time, "%Y/%m/%d %H:%M")
_time = _time + timedelta(hours=9)

table = "rakuten_hack.pc_rpp"

def scraping(keyword):
    time.sleep(1)
    #encoding
    keyword = urllib.parse.quote(keyword)
    #url
    url = "https://search.rakuten.co.jp/search/mall/{}/".format(keyword)
    print(url)
    ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36"
    headers = {"User-Agent" : ua}
    #get html
    try:
      requests = request.Request(url=url)#,headers=headers)
      #print("Request done")       
      html = request.urlopen(requests)
    except:
      print("scraping_error in Requests and urlopen")
      time.sleep(1)
      try:
        requests = request.Request(url=url,headers=headers)
        html = request.urlopen(requests)
      except:
        requests = request.Request(url)
        html = request.urlopen(requests)
    #set BueatifulSoup
    soup = BeautifulSoup(html, "html.parser")
    #definition
    names = []
    shop_names = []
    times = []
    prices = []
    rpp_ranks = []
    urls = []
    i = 1
    try:
    #get datas
      rppIndex = soup.find("div", attrs={"class", "dui-cards"})
      redirect_links = rppIndex.find_all("div", attrs={"class", "aditem"})
    except:
      rppIndex = []
      redirect_links = []
      print("error in find_all")
    try:
        for link in redirect_links:
            #print(type(link.find("div",attrs={"class","content_title"}).find("a").attrs["title"]))
            #print(link.find("div", attrs={"class", "content title"}).find('a').attrs['title'])
            names.append(link.find("div", attrs={"class", "content title"}).find("h2").find("a").text)
            #names.append(str(link.find("div", attrs={"class", "content title"}).find('a',attrs={"data-track-trigger",'title'}).text))
            #print(type(names[0]))
            shop_names.append(link.find("div", attrs={"class", "content merchant _ellipsis"}).find('a').text)
            times.append(_time)
            _price = link.find("span",attrs={"class","important"}).text.replace("円","")
            _price = _price.replace(",","")
            #print(_price)
            prices.append(_price)
            rpp_ranks.append(i)
            #img_url
            urls.append(link.find("img", attrs={"class", "_verticallyaligned"}).attrs["src"].split('?')[0])
            #link_url
            #urls.append(request.urlopen(link.attrs["href"]).geturl())
            #print(rpp_ranks)
            i+=1
    except:
      print("scraping_error")
    return names, shop_names, times, prices, rpp_ranks, urls
    
def make_df():
    #read 'keyword.txt'
    f = open('keywords.txt', 'r')
    keywords =[]
    keywords = f.readlines()
    f.close()
    #definition
    j = 0
    table_list = []

    for keyword in keywords:
        #table_list.append(str(j))
        #print(table_list[j])
        #print(type(table_list[j]))
        keyword_list = []
        keyword = keyword.replace('\n', '')
        names_, shop_names_, times_, prices_, rpp_ranks_, urls_ = scraping(keyword)
        for _ in names_:
           keyword_list.append(keyword)

        #data = {"keyword" : keyword_list , "name" : names, "shop_name" : shop_names, "created_at" : times, "price" : prices, "rpp_rank" : rpp_ranks, "img_url" : urls}
        #print(data)
        df = pd.DataFrame([])
        #df = pd.DataFrame(data)
        #names = str(names)
        #keyword_list = str(keyword_list)
        #shop_names = str(shop_names)
        _names = [str(n.encode("utf-8").decode("utf-8")) for n in names_]
        _keyword_list = [str(k) for k in keyword_list]
        _shop_names = [str(sn) for sn in shop_names_] 

        df["keyword"] = pd.Series(_keyword_list)
        df["name"] = pd.Series(_names,dtype="object")
        df["shop_name"] = pd.Series(_shop_names)
        df["created_at"] = pd.Series(times_)
        df["price"] = pd.Series(prices_)
        df["rpp_rank"] = pd.Series(rpp_ranks_)
        df["img_url"] = pd.Series(urls_)

        #df["keyword"] = pd.Series(keyword_list)
        #df["name"] = pd.Series(names)
        #df["price"] = pd.Series(prices)
        #df["rpp_rank"] = pd.Series(rpp_ranks)
        #df["shop_name"] = pd.Series(shop_names)
        #df.keyword = df.keyword
        #df.name = df.name.astype(str)
        df.shop_name = df.shop_name.astype(str)
        #df.created_at = pd.to_datetime(df.created_at)
        #print(df.price)
        df.price = df.price.astype(int)
        df.rpp_rank = df.rpp_rank.astype(int)
        df.img_url = df.img_url.astype(str)
        #print(df.dtypes)
        #print(df.dtypes)
        #print(df.columns)
        #df.to_csv("pc_{}.csv".format(keyword))
        
        try:
          gbq.to_gbq(df, table, 'rakutenhack-225301',if_exists="append", private_key="private_key.json")
        except:
          try:          
            gbq.to_gbq(df,"rakutenhack-225301",private_key="private_key.json")
          except:
            try:
               gbq.to_gbq(df,"rakutenhack-225301",if_exists="append",private_key="private_key.json")
            except:
               pd.set_option('display.max_columns', 7)
               #print(df)
               print("error")
               for index,row in df.iterrows():
                  print(type(row.created_at))
                  print(type(row.img_url))
                  print(type(row.keyword))
                  print("before name")
                  print(type(row.name))
                  print(type(row.price))
                  print(type(row.rpp_rank))
                  print(type(row.shop_name))
        del df
        #del data
        del _keyword_list,keyword_list,_names,_shop_names,times_,prices_,rpp_ranks_,urls_
        gc.collect()


if __name__ == "__main__":
    make_df()
    print('done.')
