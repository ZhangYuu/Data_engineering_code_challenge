#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 25 14:27:17 2017

@author: Memo
"""
from pandas.core.frame import DataFrame
import os
import pandas as pd
import numpy as np
import datetime

#reading each line of text data 
def get_data(address):
    data=[]
    with open(address,'r') as f:  
        lines=f.readlines()
        for line in lines:  
            data.append(line.split('|'))
    f.close()
    return data

#validating date format
def validate_date(date_str):
    try:
        datetime.datetime.strptime(date_str, '%m%d%Y')
        return True
    except ValueError:
        return False
    
#preparing data for calculating
def process_data(data,Judge):
    drop_list=[] #the list of lines we need to drop
    new_zip=[]
    new_AMT=[]
    #make a dataframe
    header=['CMTE_ID','AMNDT_IND','RPT_TP','TRANSACTION_PGI','IMAGE_NUM','TRANSACTION_TP',\
            'ENTITY_TP','NAME','CITY','STATE','ZIP_CODE','EMPLOYER','OCCUPATION','TRANSACTION_DT',\
            'TRANSACTION_AMT','OTHER_ID','TRAN_ID','FILE_NUM','MEMO_CD','MEMO_TEXT','SUB_ID']
    df=DataFrame(data,columns=header)
    #make new dataframe with the information we need
    df=df[['CMTE_ID','ZIP_CODE','TRANSACTION_DT','TRANSACTION_AMT','OTHER_ID']]
    #transfer string in TRANSACTION_AMT into int
    for i in df['TRANSACTION_AMT']:
        new_AMT.append(int(i))
    df['TRANSACTION_AMT']=new_AMT    
    #remove invalid data for medianvals_by_zip.txt
    if Judge=='zip':
        for i in list(df.index.values):
            if df.loc[i]['OTHER_ID']!='' or (len(df.loc[i]['ZIP_CODE'])>=5)==False or df.loc[i]['CMTE_ID']=='' \
            or df.loc[i]['TRANSACTION_AMT']=='':
                drop_list.append(i)
            else:
                new_zip.append(df.loc[i]['ZIP_CODE'][0:5])
    #remove invalid data for medianvals_by_date.txt
    if Judge=='date':
        for i in list(df.index.values):
            if df.loc[i]['OTHER_ID']!='' or validate_date(df.loc[i]['TRANSACTION_DT'])!=True or df.loc[i]['CMTE_ID']=='' \
            or df.loc[i]['TRANSACTION_AMT']=='':
                drop_list.append(i)    
    #remove invalid line in drop list
    df.drop(drop_list,inplace=True)
    #Add number of 
    if Judge=='zip':
        df['ZIP_CODE']=new_zip
        df['NO']=1
    return df

#round and transfer value in list
def round_float(lis):
    round_lis=[]
    for i in lis:
        round_lis.append(int(round(i)))
    return round_lis

#medianvals_by_zip
def by_zip(output_path_zip,df):
    #calculate total number of transactions and amount of contributions received by recipient from the contributor's zip code streamed in so far
    a=df.groupby(['CMTE_ID','ZIP_CODE'])['NO','TRANSACTION_AMT'].agg('cumsum')
    df[['CUM_Number','CUM_SUM']]=a[['NO','TRANSACTION_AMT']]
    #calculate running median
    median=df.groupby(['CMTE_ID','ZIP_CODE']).\
    apply(lambda x: pd.DataFrame([(x.index[i], np.median(x["TRANSACTION_AMT"].iloc[0:i+1])) for i in range(len(x))]))
    #sort running median with index
    median=median.sort_values(by=[0])
    df['CUM_Median']=round_float(median[1].values)
    df=df[['CMTE_ID','ZIP_CODE','CUM_Median','CUM_Number','CUM_SUM']]
    #write text output
    df.to_csv(output_path_zip,header=False,index=False,sep='|')


#medianvals_by_date
def by_date(output_path_date,df):
    df=df.groupby(['CMTE_ID','TRANSACTION_DT'])['TRANSACTION_AMT'].agg(['median','size','sum']).reset_index()
    df['median']=round_float(df['median'])
    #write text output
    df.to_csv(output_path_date,header=False,index=False,sep='|')


if __name__ == "__main__":
    #get current document address
    #local_address=os.path.abspath(os.path.join(os.path.dirname('find_political_donors.py'),os.path.pardir)) 
    data=get_data('input/itcont.txt')
    df=process_data(data,'zip')
    by_zip('output/medianvals_by_zip.txt',df)
    df=process_data(data,'date')
    by_date('output/medianvals_by_date.txt',df)
