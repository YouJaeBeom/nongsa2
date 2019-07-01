#!/usr/bin/env python
# coding: utf-8

# In[ ]:

import json
import urllib.request 
import pymysql
import time
from pyproj import Proj, transform
import numpy as np
import pandas  as pd
url5="http://dbwo4011.cafe24.com/migration/resoures_info.json"
url_query = url5 


#검색 요청 및 처리
response = urllib.request.urlopen(url_query)
print(response)
byte_data = response.read()
data = byte_data.decode('utf-8')


json_data = json.loads(data)


json_object=json_data["data"]
connection=pymysql.connect(host='dbwo4011.cafe24.com',user='dbwo4011',password='ajswl1234', db='dbwo4011',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
for data in json_object:
    VILL_ID=str(data["VILL_ID"])
    VILL_ID=VILL_ID.replace("\n"," ")
    VILL_NM=str(data["VILL_NM"])
    VILL_NM=VILL_NM.replace("\n"," ")
    VILL_NATURE_RESOURCE=str(data["VILL_NATURE_RESOURCE"])
    VILL_NATURE_RESOURCE=VILL_NATURE_RESOURCE.replace("\n"," ")
    VILL_ECONOMY_RESOURCE=str(data["VILL_ECONOMY_RESOURCE"])
    VILL_ECONOMY_RESOURCE=VILL_ECONOMY_RESOURCE.replace("\n"," ")
    
    VILL_NATURE_RESOURCE1=str(data["VILL_NATURE_RESOURCE1"])
    VILL_NATURE_RESOURCE1=VILL_NATURE_RESOURCE1.replace("\n"," ")
    VILL_NATURE_RESOURCE2=str(data["VILL_NATURE_RESOURCE2"])
    VILL_NATURE_RESOURCE2=VILL_NATURE_RESOURCE2.replace("\n"," ")
    
    VILL_ECONOMY_RESOURCE1=str(data["VILL_ECONOMY_RESOURCE1"])
    VILL_ECONOMY_RESOURCE1=VILL_ECONOMY_RESOURCE1.replace("\n"," ")
    VILL_ECONOMY_RESOURCE2=str(data["VILL_ECONOMY_RESOURCE2"])
    VILL_ECONOMY_RESOURCE2=VILL_ECONOMY_RESOURCE2.replace("\n"," ")
    VILL_ECONOMY_RESOURCE3=str(data["VILL_ECONOMY_RESOURCE3"])
    VILL_ECONOMY_RESOURCE3=VILL_ECONOMY_RESOURCE3.replace("\n"," ")
    
    
    VILL_COMMUNITY_RESOURCE=str(data["VILL_COMMUNITY_RESOURCE"])
    VILL_COMMUNITY_RESOURCE=VILL_COMMUNITY_RESOURCE.replace("\n"," ")
    VILL_COMMUNITY_RESOURCE1=str(data["VILL_COMMUNITY_RESOURCE1"])
    VILL_COMMUNITY_RESOURCE1=VILL_COMMUNITY_RESOURCE1.replace("\n"," ")
    VILL_COMMUNITY_RESOURCE2=str(data["VILL_COMMUNITY_RESOURCE2"])
    VILL_COMMUNITY_RESOURCE2=VILL_COMMUNITY_RESOURCE2.replace("\n"," ")
    VILL_COMMUNITY_RESOURCE3=str(data["VILL_COMMUNITY_RESOURCE3"])
    VILL_COMMUNITY_RESOURCE3=VILL_COMMUNITY_RESOURCE3.replace("\n"," ")
    VILL_COMMUNITY_RESOURCE4=str(data["VILL_COMMUNITY_RESOURCE4"])
    VILL_COMMUNITY_RESOURCE4=VILL_COMMUNITY_RESOURCE4.replace("\n"," ")
    try:
        urlz="http://www.juso.go.kr/addrlink/addrLinkApi.do?currentPage=1&countPerPage=10&confmKey=U01TX0FVVEgyMDE5MDYzMDIxMTIyMDEwODg0NjA=&resultType=json&keyword="
        # roadname 넣기
        url2 = urllib.parse.quote_plus(VILL_NM)
        url_query = urlz +url2

        #검색 요청 및 처리
        response = urllib.request.urlopen(url_query)
        print(response)
        byte_data = response.read()
        data = byte_data.decode('utf-8')
        json_data = json.loads(data)
        json_object=json_data["results"]["juso"]
    
        admCd=[]
        rnMgtSn=[]
        udrtYn=[]
        buldMnnm=[]
        buldSlno=[]
        resultType="json"

        for data in json_object:
            admCd.append(str(data["admCd"]))
            rnMgtSn.append(str(data["rnMgtSn"]))
            udrtYn.append(str(data["udrtYn"]))
            buldMnnm.append((data["buldMnnm"])) # number
            buldSlno.append((data["buldSlno"])) # number
        print("admCd",admCd[0])
        print("rnMgtSn",rnMgtSn[0])
        print("udrtYn",udrtYn[0])
        print("buldMnnm",buldMnnm[0])
        print("buldSlno",buldSlno[0])
        try:
            urlz="http://www.juso.go.kr/addrlink/addrCoordApi.do?currentPage=1&countPerPage=10&confmKey=U01TX0FVVEgyMDE5MDYzMDIzMDczNjEwODg0NjI=&resultType=json"
            # roadname 넣기
            url_query = urlz +"&admCd="+admCd[0]+"&rnMgtSn="+rnMgtSn[0]+"&udrtYn="+udrtYn[0]+"&buldMnnm="+buldMnnm[0]+"&buldSlno="+buldSlno[0]


            #검색 요청 및 처리
            response = urllib.request.urlopen(url_query)
            print(response)
            byte_data = response.read()
            data = byte_data.decode('utf-8')


            json_data = json.loads(data)


            json_object=json_data["results"]["juso"]
            connection=pymysql.connect(host='dbwo4011.cafe24.com',user='dbwo4011',password='ajswl1234', db='dbwo4011',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
            for data in json_object:
                entX=(str(data["entX"]))
                entY=(str(data["entY"]))
                # Projection 정의
                # UTM-K
                proj_UTMK = Proj(init='epsg:5178') # UTM-K(Bassel) 도로명주소 지도 사용 중

                # WGS1984
                proj_WGS84 = Proj(init='epsg:4326') # Wgs84 경도/위도, GPS사용 전지구 좌표
                
                Iongitude, Latitude = transform(proj_UTMK,proj_WGS84,entX,entY)
                print(Iongitude)
                print(Latitude)
            with connection.cursor() as cursor:
                    sql = "INSERT INTO `city_info` ( `VILL_ID` , `VILL_NM`  , `VILL_NATURE_RESOURCE` , `VILL_ECONOMY_RESOURCE`  , `VILL_NATURE_RESOURCE1` , `VILL_NATURE_RESOURCE2` ,  `VILL_ECONOMY_RESOURCE1` , `VILL_ECONOMY_RESOURCE2` , `VILL_ECONOMY_RESOURCE3` ,  `VILL_COMMUNITY_RESOURCE` , `VILL_COMMUNITY_RESOURCE1` , `VILL_COMMUNITY_RESOURCE2` , `VILL_COMMUNITY_RESOURCE3` , `VILL_COMMUNITY_RESOURCE4` ,`Latitude`,`Iongitude`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor.execute(sql,(VILL_ID , VILL_NM  , VILL_NATURE_RESOURCE , VILL_ECONOMY_RESOURCE  , VILL_NATURE_RESOURCE1 , VILL_NATURE_RESOURCE2 , VILL_ECONOMY_RESOURCE1 , VILL_ECONOMY_RESOURCE2 , VILL_ECONOMY_RESOURCE3  , VILL_COMMUNITY_RESOURCE , VILL_COMMUNITY_RESOURCE1 , VILL_COMMUNITY_RESOURCE2, VILL_COMMUNITY_RESOURCE3 , VILL_COMMUNITY_RESOURCE4,Latitude,Iongitude))
                    connection.commit()
                    
      
        
        except:
            print("좌표못찾음")
            with connection.cursor() as cursor:
                    sql = "INSERT INTO `city_info` ( `VILL_ID` , `VILL_NM`  , `VILL_NATURE_RESOURCE` , `VILL_ECONOMY_RESOURCE`  , `VILL_NATURE_RESOURCE1` , `VILL_NATURE_RESOURCE2` ,  `VILL_ECONOMY_RESOURCE1` , `VILL_ECONOMY_RESOURCE2` , `VILL_ECONOMY_RESOURCE3` ,  `VILL_COMMUNITY_RESOURCE` , `VILL_COMMUNITY_RESOURCE1` , `VILL_COMMUNITY_RESOURCE2` , `VILL_COMMUNITY_RESOURCE3` , `VILL_COMMUNITY_RESOURCE4` ,`Latitude`,`Iongitude`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor.execute(sql,(VILL_ID , VILL_NM  , VILL_NATURE_RESOURCE , VILL_ECONOMY_RESOURCE  , VILL_NATURE_RESOURCE1 , VILL_NATURE_RESOURCE2 , VILL_ECONOMY_RESOURCE1 , VILL_ECONOMY_RESOURCE2 , VILL_ECONOMY_RESOURCE3  , VILL_COMMUNITY_RESOURCE , VILL_COMMUNITY_RESOURCE1 , VILL_COMMUNITY_RESOURCE2, VILL_COMMUNITY_RESOURCE3 , VILL_COMMUNITY_RESOURCE4,Latitude,Iongitude))
                    connection.commit()
                   
    except:
        print("주소못찾음")
        with connection.cursor() as cursor:
                    sql = "INSERT INTO `city_info` ( `VILL_ID` , `VILL_NM`  , `VILL_NATURE_RESOURCE` , `VILL_ECONOMY_RESOURCE`  , `VILL_NATURE_RESOURCE1` , `VILL_NATURE_RESOURCE2` ,  `VILL_ECONOMY_RESOURCE1` , `VILL_ECONOMY_RESOURCE2` , `VILL_ECONOMY_RESOURCE3` ,  `VILL_COMMUNITY_RESOURCE` , `VILL_COMMUNITY_RESOURCE1` , `VILL_COMMUNITY_RESOURCE2` , `VILL_COMMUNITY_RESOURCE3` , `VILL_COMMUNITY_RESOURCE4` ,`Latitude`,`Iongitude`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor.execute(sql,(VILL_ID , VILL_NM  , VILL_NATURE_RESOURCE , VILL_ECONOMY_RESOURCE  , VILL_NATURE_RESOURCE1 , VILL_NATURE_RESOURCE2 , VILL_ECONOMY_RESOURCE1 , VILL_ECONOMY_RESOURCE2 , VILL_ECONOMY_RESOURCE3  , VILL_COMMUNITY_RESOURCE , VILL_COMMUNITY_RESOURCE1 , VILL_COMMUNITY_RESOURCE2, VILL_COMMUNITY_RESOURCE3 , VILL_COMMUNITY_RESOURCE4,Latitude,Iongitude))
                    connection.commit()
                    
                    
                    
    

    


