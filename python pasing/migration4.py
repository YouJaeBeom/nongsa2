#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#import
from pyproj import Proj, transform
import numpy as np
import pandas  as pd
import urllib.request
import json 
import xml.etree.ElementTree as ET
import pymysql
import time
#도서검색 url
# Projection 정의
# UTM-K
proj_UTMK = Proj(init='epsg:5178') # UTM-K(Bassel) 도로명주소 지도 사용 중

# WGS1984
proj_WGS84 = Proj(init='epsg:4326') # Wgs84 경도/위도, GPS사용 전지구 좌표
url1= "http://211.237.50.150:7080/openapi/52b33180da364563a5a8ef5327813d368fa37f2d05b87c0866b94e4a88e6eab4/json/Grid_20150914000000000230_1/1/1000?SIDO_NM="


url2 = urllib.parse.quote_plus("경상북도")

#완성된 url
FullURL = url1 + url2


#검색 요청 및 처리
response = urllib.request.urlopen(FullURL)
print(response)
rescode = response.getcode()
response_body = response.read()

data = response_body.decode('utf-8')
json_data = json.loads(data)
json_object=json_data["Grid_20150914000000000230_1"]["row"]

connection=pymysql.connect(host='dbwo4011.cafe24.com',user='dbwo4011',password='ajswl1234', db='dbwo4011',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
for data in json_object: 
    
    SIDO_NM=str(data["SIDO_NM"])
    
    SIGUN_NM=str(data["SIGUN_NM"])
    
    ADDR=str(data["ADDR"])
   
    DEAL_AMOUNT=str(data["DEAL_AMOUNT"])
   
    DEAL_BIGO=str(data["DEAL_BIGO"])
   
    BUILDING_AREA=str(data["BUILDING_AREA"])

    AREA_ETC=str(data["AREA_ETC"])
    
    BUILD_YEAR=str(data["BUILD_YEAR"])
    
    VACANT_YEAR=str(data["VACANT_YEAR"])
    
    STRUCT_TYPE=str(data["STRUCT_TYPE"])
    
    OWNER_NM=str(data["OWNER_NM"])
    
    OWNER_CONTACT=str(data["OWNER_CONTACT"])
    
    INSPECTOR=str(data["INSPECTOR"])
   
    LOT_AREA=str(data["LOT_AREA"])
    
    BIGO=str(data["BIGO"])
    
    FILE_PATH1=str(data["FILE_PATH1"])
    
    FILE_PATH2=str(data["FILE_PATH2"])
    
    FILE_PATH3=str(data["FILE_PATH3"])
    
    DETAIL_URL=str(data["DETAIL_URL"])
   
    DEAL_NEGO_YN=str(data["DEAL_NEGO_YN"])
    
    LEASE_AMOUNT=str(data["LEASE_AMOUNT"])
   
    RENT_AMOUNT=str(data["RENT_AMOUNT"])
    
    GUBUN=str(data["GUBUN"])
    
    DEAL_TYPE=str(data["DEAL_TYPE"])
    
    REG_DT=str(data["REG_DT"])
    try:
        urlz="http://www.juso.go.kr/addrlink/addrLinkApi.do?currentPage=1&countPerPage=10&confmKey=U01TX0FVVEgyMDE5MDYzMDIxMTIyMDEwODg0NjA=&resultType=json&keyword="
        # roadname 넣기
        url2 = urllib.parse.quote_plus(ADDR)
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
                    sql = "INSERT INTO `migration_info` (`ID`,`SIDO_NM`,`SIGUN_NM`,`ADDR`,`DEAL_AMOUNT`,`DEAL_BIGO`,`BUILDING_AREA`,`AREA_ETC`,`BUILD_YEAR`,`VACANT_YEAR`,`STRUCT_TYPE`,`OWNER_NM`,`OWNER_CONTACT`,`INSPECTOR`,`LOT_AREA`,`BIGO`,`FILE_PATH1`,`FILE_PATH2`,`FILE_PATH3`,`DETAIL_URL`,`DEAL_NEGO_YN`,`LEASE_AMOUNT`,`RENT_AMOUNT`,`GUBUN`,`DEAL_TYPE`,`REG_DT`,`Latitude`,`Iongitude`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor.execute(sql,("농림수산식품교육문화정보원",SIDO_NM,SIGUN_NM,ADDR,DEAL_AMOUNT,DEAL_BIGO,BUILDING_AREA,AREA_ETC,BUILD_YEAR,VACANT_YEAR,STRUCT_TYPE,OWNER_NM,OWNER_CONTACT,INSPECTOR,LOT_AREA,BIGO,FILE_PATH1,FILE_PATH2,FILE_PATH3,DETAIL_URL,DEAL_NEGO_YN,LEASE_AMOUNT,RENT_AMOUNT,GUBUN,DEAL_TYPE,REG_DT,Latitude,Iongitude))
                    connection.commit()
                    time.sleep(1)
      
        
        except:
            print("좌표못찾음")
            with connection.cursor() as cursor:
                    sql = "INSERT INTO `migration_info` (`ID`,`SIDO_NM`,`SIGUN_NM`,`ADDR`,`DEAL_AMOUNT`,`DEAL_BIGO`,`BUILDING_AREA`,`AREA_ETC`,`BUILD_YEAR`,`VACANT_YEAR`,`STRUCT_TYPE`,`OWNER_NM`,`OWNER_CONTACT`,`INSPECTOR`,`LOT_AREA`,`BIGO`,`FILE_PATH1`,`FILE_PATH2`,`FILE_PATH3`,`DETAIL_URL`,`DEAL_NEGO_YN`,`LEASE_AMOUNT`,`RENT_AMOUNT`,`GUBUN`,`DEAL_TYPE`,`REG_DT`,`Latitude`,`Iongitude`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor.execute(sql,("농림수산식품교육문화정보원",SIDO_NM,SIGUN_NM,ADDR,DEAL_AMOUNT,DEAL_BIGO,BUILDING_AREA,AREA_ETC,BUILD_YEAR,VACANT_YEAR,STRUCT_TYPE,OWNER_NM,OWNER_CONTACT,INSPECTOR,LOT_AREA,BIGO,FILE_PATH1,FILE_PATH2,FILE_PATH3,DETAIL_URL,DEAL_NEGO_YN,LEASE_AMOUNT,RENT_AMOUNT,GUBUN,DEAL_TYPE,REG_DT,"0","0"))
                    connection.commit()
                    time.sleep(1)
    except:
        print("주소못찾음")
        with connection.cursor() as cursor:
                    sql = "INSERT INTO `migration_info` (`ID`,`SIDO_NM`,`SIGUN_NM`,`ADDR`,`DEAL_AMOUNT`,`DEAL_BIGO`,`BUILDING_AREA`,`AREA_ETC`,`BUILD_YEAR`,`VACANT_YEAR`,`STRUCT_TYPE`,`OWNER_NM`,`OWNER_CONTACT`,`INSPECTOR`,`LOT_AREA`,`BIGO`,`FILE_PATH1`,`FILE_PATH2`,`FILE_PATH3`,`DETAIL_URL`,`DEAL_NEGO_YN`,`LEASE_AMOUNT`,`RENT_AMOUNT`,`GUBUN`,`DEAL_TYPE`,`REG_DT`,`Latitude`,`Iongitude`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor.execute(sql,("농림수산식품교육문화정보원",SIDO_NM,SIGUN_NM,ADDR,DEAL_AMOUNT,DEAL_BIGO,BUILDING_AREA,AREA_ETC,BUILD_YEAR,VACANT_YEAR,STRUCT_TYPE,OWNER_NM,OWNER_CONTACT,INSPECTOR,LOT_AREA,BIGO,FILE_PATH1,FILE_PATH2,FILE_PATH3,DETAIL_URL,DEAL_NEGO_YN,LEASE_AMOUNT,RENT_AMOUNT,GUBUN,DEAL_TYPE,REG_DT,"0","0"))
                    connection.commit()
                    time.sleep(1)
    


   
    




