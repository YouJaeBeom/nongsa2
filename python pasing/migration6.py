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



url1 = "http://211.237.50.150:7080/openapi/52b33180da364563a5a8ef5327813d368fa37f2d05b87c0866b94e4a88e6eab4/xml/Grid_20150914000000000230_1/1/1000?SIDO_NM="


url2 = urllib.parse.quote_plus("전라북도")

#완성된 url
FullURL = url1 + url2


#검색 요청 및 처리
response = urllib.request.urlopen(FullURL)
print(response)
rescode = response.getcode()
response_body = response.read()


stuff = ET.fromstring(response_body.decode('utf-8')) # XML을 읽을 수 있게 트리형태로 변환
print(stuff)
lst = stuff.findall('row') 
print(lst)
print('The number of nations:', len(lst)) 
connection=pymysql.connect(host='dbwo4011.cafe24.com',user='dbwo4011',password='ajswl1234', db='dbwo4011',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
for item in lst: 
    print('SIDO_NM', item.find('SIDO_NM').text)
    SIDO_NM=item.find('SIDO_NM').text
    print('SIGUN_NM', item.find('SIGUN_NM').text)
    SIGUN_NM=item.find('SIGUN_NM').text
    print('ADDR', item.find('ADDR').text)
    ADDR=item.find('ADDR').text
    print('DEAL_AMOUNT', item.find('DEAL_AMOUNT').text)
    DEAL_AMOUNT=item.find('DEAL_AMOUNT').text
    print('DEAL_BIGO', item.find('DEAL_BIGO').text)
    DEAL_BIGO=item.find('DEAL_BIGO').text
    print('BUILDING_AREA', item.find('BUILDING_AREA').text)
    BUILDING_AREA=item.find('BUILDING_AREA').text
    print('AREA_ETC', item.find('AREA_ETC').text)
    AREA_ETC=item.find('AREA_ETC').text
    print('BUILD_YEAR', item.find('BUILD_YEAR').text)
    BUILD_YEAR=item.find('BUILD_YEAR').text
    print('VACANT_YEAR', item.find('VACANT_YEAR').text)
    VACANT_YEAR=item.find('VACANT_YEAR').text
    print('STRUCT_TYPE', item.find('STRUCT_TYPE').text)
    STRUCT_TYPE=item.find('STRUCT_TYPE').text
    print('OWNER_NM', item.find('OWNER_NM').text)
    OWNER_NM=item.find('OWNER_NM').text
    print('OWNER_CONTACT', item.find('OWNER_CONTACT').text)
    OWNER_CONTACT=item.find('OWNER_CONTACT').text
    print('INSPECTOR', item.find('INSPECTOR').text)
    INSPECTOR=item.find('INSPECTOR').text
    print('LOT_AREA', item.find('LOT_AREA').text)
    LOT_AREA=item.find('LOT_AREA').text
    print('BIGO', item.find('BIGO').text)
    BIGO=item.find('BIGO').text
    print('FILE_PATH1', item.find('FILE_PATH1').text)
    FILE_PATH1=item.find('FILE_PATH1').text
    print('FILE_PATH2', item.find('FILE_PATH2').text)
    FILE_PATH2=item.find('FILE_PATH2').text
    print('FILE_PATH3', item.find('FILE_PATH3').text)
    FILE_PATH3=item.find('FILE_PATH3').text
    print('DETAIL_URL', item.find('DETAIL_URL').text)
    DETAIL_URL=item.find('DETAIL_URL').text
    print('DEAL_NEGO_YN', item.find('DEAL_NEGO_YN').text)
    DEAL_NEGO_YN=item.find('DEAL_NEGO_YN').text
    print('LEASE_AMOUNT', item.find('LEASE_AMOUNT').text)
    LEASE_AMOUNT=item.find('LEASE_AMOUNT').text
    print('RENT_AMOUNT', item.find('RENT_AMOUNT').text)
    RENT_AMOUNT=item.find('RENT_AMOUNT').text
    print('GUBUN', item.find('GUBUN').text)
    GUBUN=item.find('GUBUN').text
    print('DEAL_TYPE', item.find('DEAL_TYPE').text)
    DEAL_TYPE=item.find('DEAL_TYPE').text
    print('REG_DT', item.find('REG_DT').text)
    REG_DT=item.find('REG_DT').text
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
                entX=((data["entX"]))
                entY=((data["entY"]))
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
    

   
    




