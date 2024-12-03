# -*- coding: utf-8 -*-


import numpy as np
import pandas as pd
from datetime import datetime,date,timedelta,time
import os
import requests,json
from requests.exceptions import InvalidJSONError
import app_config.app_config as cfg
config=cfg.getconfig()
import timeseries.timeseries as ts
meta=ts.timeseriesmeta()
qr=ts.timeseriesquery()
import boto3
import botocore
import re
import email as em
import pickle
from dotenv import load_dotenv
load_dotenv()

AWSAccessKeyId =os.getenv('AWS_ACCESS_KEY_ID')
AWSSecretKey =os.getenv('AWS_SECRET_ACCESS_KEY')
Bucket_Name =os.getenv('BUCKET_NAME')
 
session = boto3.Session(aws_access_key_id=AWSAccessKeyId, aws_secret_access_key=AWSSecretKey)

s3 = session.resource("s3")
try:
	with open('files_parsed.pkl', 'rb') as f:
		file_list = pickle.load(f)			
except IOError as e:
	file_list = []
	with open('files_parsed.pkl', 'wb') as f:
		pickle.dump([], f, pickle.HIGHEST_PROTOCOL)

def persist_pickle_file(file_list):
	with open('files_parsed.pkl', 'wb') as f:
		pickle.dump(file_list, f, pickle.HIGHEST_PROTOCOL)

get_last_modified = lambda obj: int(obj.last_modified.strftime('%s'))

bckt = s3.Bucket(Bucket_Name)
objs = [obj for obj in bckt.objects.all()]
objs = [obj for obj in sorted(objs, key=get_last_modified, reverse=True)][0:150]


FOLDER_PATH = "/space/es-master/src/excel-parsers/Abhishek/"
csv_file = "Quality_parameters_SIDHI.csv"
csv_path = os.path.join(FOLDER_PATH, csv_file)

def process_excel_file(filename):
    file_path = FOLDER_PATH +filename
    column_mappings = [
        {
            "Residue 90 %": "SDCW2_QCX_RM1_R-90micron",
            "Residue 212 %": "SDCW2_QCX_RM1_R-212micron",
            "SiO2 %": "SDCW2_QCX_RM1_SIO2",
            "Al2O3 %": "SDCW2_QCX_RM1_Al2O3",
            "Fe2O3 %": "SDCW2_QCX_RM1_Fe2O3",
            "CaO %": "SDCW2_QCX_RM1_CaO/tc",
            "K2O %": "SDCW2_QCX_RM1_K2O",
            "Na2O %": "SDCW2_QCX_RM1_Na2O",
            "Chloride": "SDCW2_QCX_RM1_Cl",
            "R.M. SO3 %": "SDCW2_QCX_RM1_SO3",
            "P2O5 %": "SDCW2_QCX_RM1_P2O5",
            "LSF": "SDCW2_QCX_RM1_LSF",
            "SM": "SDCW2_QCX_RM1_SM",
            "AM": "SDCW2_QCX_RM1_AM"
        },
        {
            "Residue 90 %": "SDCW2_QCX_KN_R_90micron",
            "Residue 212 %": "SDCW2_QCX_KN_R_212micron",
            "SiO2 %": "SDCW2_QCX_KN_SIO2",
            "Al2O3 %": "SDCW2_QCX_KN_Al2O3",
            "Fe2O3 %": "SDCW2_QCX_KN_Fe2O3",
            "CaO %": "SDCW2_QCX_KN_CaO",
            "K2O %": "SDCW2_QCX_KN_K2O",
            "Na2O %": "SDCW2_QCX_KN_Na2O",
            "Chloride": "SDCW2_QCX_KN_Cl",
            "R.M. SO3 %": "SDCW2_QCX_KN_SO3",
            "P2O5 %": "SDCW2_QCX_KN_P2O5",
            "LSF": "SDCW2_QCX_KN_LSF",
            "SM": "SDCW2_QCX_KN_SM",
            "AM": "SDCW2_QCX_KN_AM"
        },
        {
            "Liter Wt g/l": "SDCW2_QCX_CLK_LTR.WT",
            "SiO2 %": "SDCW2_QCX_CLK_SIO2",
            "Al2O3 %": "SDCW2_QCX_CLK_Al2O3",
            "Fe2O3 %": "SDCW2_QCX_CLK_Fe2O3",
            "CaO %": "SDCW2_QCX_CLK_CaO",
            "F.CaO %": "SDCW2_QCX_CLK_F.CaO",
            "LSF": "SDCW2_QCX_CLK_LSF",
            "SM": "SDCW2_QCX_CLK_SM",
            "AM": "SDCW2_QCX_CLK_AM",
            "C3S": "SDCW2_QCX_CLK_C3S",
            "C2S": "SDCW2_QCX_CLK_C2S",
            "C3A": "SDCW2_QCX_CLK_C3A",
            "C4AF": "SDCW2_QCX_CLK_C4AF",
            "Liquid %": "SDCW2_QCX_CLK_Liquid",
	    "LOI %": "SDCW2_QCX_CLK_LOI"
        },
        {
            "LOI-KF %": "SDCW2_QCX_HM_LOI-KF",
            "LOI-HM %": "SDCW2_QCX_HM_LOI-HM",
            "DOC %": "SDCW2_QCX_HM_DOC",
            "SO3 %": "SDCW2_QCX_HM_SO3",
            "Cl %": "SDCW2_QCX_HM_Cl",
            "Na2O %": "SDCW2_QCX_HM_Na2O",
            "K2O %": "SDCW2_QCX_HM_K2O",
            "A/S": "SDCW2_QCX_HM_A.S"
        },
        {
            "SiO2 %": "SDCW2_QCX_LST_SIO2",
            "Al2O3 %": "SDCW2_QCX_LST_Al2O3",
            "Fe2O3 %": "SDCW2_QCX_LST_Fe2O3",
            "CaO %": "SDCW2_QCX_LST_CaO",
            "K2O %": "SDCW2_QCX_LST_K2O",
            "Na2O %": "SDCW2_QCX_LST_Na2O",
            "Chloride": "SDCW2_QCX_LST_Cl",
            "SM": "SDCW2_QCX_LST_SM",
            "AM": "SDCW2_QCX_LST_AM"
        },
        {
            "Blaine  M2/Kg": "SDCW2_QCX_CM_BLAINE"
        },
        {
            "GCV": "SDCW2_QCX_KM_GCV",
            "ASH": "SDCW2_QCX_KM_ASH"
        }
    ]
    file_instance = pd.ExcelFile(file_path)
    df_dict = {}
    for i, sheet_name in enumerate(file_instance.sheet_names):
        df = pd.read_excel(file_path, sheet_name=sheet_name, index_col=None)
        df.columns = df.iloc[0]
        df = df.iloc[1:]  
        df.reset_index(drop=True, inplace=True)        
        if i < len(column_mappings):
            column_mapping = column_mappings[i]
            df.columns = [column_mapping.get(col, col) for col in df.columns]        
        df_dict[sheet_name] = df    
    combined_df = None    
    for sheet_name, df in df_dict.items():
        if combined_df is None:
            combined_df = df
        else:
            combined_df = pd.merge(combined_df, df, on='Date', how='outer')
   
    #combined_df['Date'] = pd.to_datetime(combined_df['Date'], format="%d/%m/%Y", errors='coerce')	    
    #combined_df['Date'] = combined_df['Date'].dt.strftime("%d-%m-%Y")    
    combined_df["time"] = pd.to_datetime(combined_df["Date"], errors='coerce').values.astype(np.int64) // 10**6
    combined_df["time"] -= int(5.5 * 60 * 60 * 1000)   
    #combined_df.replace('-', 0, inplace=True)
    combined_df.replace('-', np.nan, inplace=True)	    
    combined_df.drop('Date', axis=1, inplace=True)    
    combined_df = combined_df.round(2)
    #print(combined_df)    
    return combined_df


def convert_to_2hrs_post_updated_data(combined_df, last_updated_time):
    updated_df = combined_df[combined_df['time'] > last_updated_time]
    merged_df = pd.DataFrame()
    for i in range(len(combined_df)):
        start_time = combined_df['time'].iloc[i]
        if start_time > last_updated_time:
            end_time = start_time + 86340000
	        # print(start_time)
            times = range(start_time, end_time + 1, 60000)  # 7200000 milliseconds = 2 hrs      #60000 milliseconds = 1 minute
            df123 = pd.DataFrame(times, columns=['time'])
            merged_df = pd.merge(df123, combined_df, on='time', how='left')
            merged_df = merged_df.ffill()
            for col in merged_df.columns:
                if "SDCW2" in col:
                    print(col)
                    df2 = merged_df[["time", col]].dropna()
                    data = df2[["time", col]].values.tolist()
                    name =  col
                    body=[{"name":name,"datapoints":data, "tags" : {"type":"TEST"}}]
                    url = 'http://10.13.1.221/exactdata/api/v1/datapoints/'
                    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
                    print (requests.post(url, json=body,headers=headers).status_code)            
        else:
            print("No new data found greater than last_updated_time.")
    return merged_df,updated_df




def convert_to_2hrs_post_data(combined_df):
    for i in range(len(combined_df)):
        start_time = combined_df['time'].iloc[i]
        end_time = start_time + 86340000
        times = range(start_time, end_time + 1, 60000)  # 7200000 milliseconds = 2 hrs      #60000 milliseconds = 1 minute
        df123 = pd.DataFrame(times, columns=['time'])
        merged_df = pd.merge(df123, combined_df, on='time', how='left')
        merged_df = merged_df.ffill()
        for col in merged_df.columns:
            if "SDCW2" in col:
                print(col)
                df2 = merged_df[["time", col]].dropna()
                data = df2[["time", col]].values.tolist()
                name =  col
                body=[{"name":name,"datapoints":data, "tags" : {"type":"TEST"}}]
                url = 'http://10.13.1.221/exactdata/api/v1/datapoints/'
                headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
                print (requests.post(url, json=body,headers=headers).status_code)
    return merged_df


#filename = "Quality_parameters_SIDHI.xlsx"
# combined_df = process_excel_file(filename)
# if os.path.isfile(csv_path):
#     print("\n\n CSV is Found\n\n")
#     new_df = pd.read_csv(csv_path)
#     last_updated_time = new_df['time'].iloc[-1]
#     print(last_updated_time)
#     merged_df,updated_df=convert_to_2hrs_post_updated_data(combined_df, last_updated_time)
#     #print(merged_df)
#     updated_df.to_csv("Quality_parameters_SIDHI.csv",index=False)
# else:
#     merged_df=convert_to_2hrs_post_data(combined_df)
#     combined_df.to_csv("Quality_parameters_SIDHI.csv",index=False)


for obj in objs:
    
        print (obj.key)
        s3.Bucket(Bucket_Name).download_file(obj.key, 'local_email_copy.txt')
        msg = em.message_from_file(open('local_email_copy.txt'))
        # print(msg['Subject'])
        # print(msg['From'])
        msg_From = re.findall("[a-zA-Z0-9._]*@[a-zA-Z0-9._]*", msg['From'])

        if "Sidhi-Line-2" in msg['Subject']:
            if "utssq0s9298lgvbgilmsc2cml95gpgfv36c0kd01" in obj.key:
                continue
            print ("Sidhi-Line-2 Quality Report Found")
            attachment = msg.get_payload()[1]
            msg['Subject'] = msg['Subject'].replace("Fwd: ","")
            msg['Subject'] = msg['Subject'].replace("RE: ", "re")
            print (msg['Subject'], type(msg['Subject']))
            fyl = open("Quality_parameters_SIDHI.xlsx","w")
            print(msg['Subject'])
            print(msg_From)
            filename ="Quality_parameters_SIDHI.xlsx"
            fyl.write(attachment.get_payload(decode=True)) 
            fyl.close()
            combined_df=process_excel_file(filename)
            if os.path.isfile(csv_path):
                print("\n\n CSV is Found\n\n")
                new_df = pd.read_csv(csv_path)
                last_updated_time = new_df['time'].iloc[-1]
                print(last_updated_time)
                merged_df,updated_df=convert_to_2hrs_post_updated_data(combined_df, last_updated_time)
                #print(merged_df)
                updated_df.to_csv("Quality_parameters_SIDHI.csv",index=False)
                print("\n\n Updated CSV is Created \n\n")
            else:
                merged_df=convert_to_2hrs_post_data(combined_df)
                combined_df.to_csv("Quality_parameters_SIDHI.csv",index=False)
                print("\n\n New CSV is created \n\n")
            #time.sleep(5)
            # os.remove(filename)

#changes: line 156 -> replacing with NaN instead of 0 for the value '-'
# 1 minute data push instead of 2 hour line 197 and like 172