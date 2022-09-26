#!/usr/bin/env python
# coding: utf-8

# In[50]:


#!/usr/bin/env python
# coding: utf-8

# In[14]:


#!/usr/bin/env python
# coding: utf-8

# In[84]:


import numpy as np
import pandas as pd
import fsspec
import time
from google.cloud import bigquery
from datetime import datetime
from datetime import date as d
import datetime as dt
import os
from google.oauth2 import service_account
from google.cloud import storage
from datetime import timedelta


# In[138]:

dates = d.today()
times = datetime.now()

key_path='C:/Users/roop.sidhu_spicemone/Downloads/roop-sidhu.json'
#f = open('/home/sdlreco/crons/ybl_aeps/stat/stat-'+str(dates)+'.txt', 'a+')
#f.close()

def main():
    
    date = d.today()-timedelta(1)
    current_date5 = date.strftime('%d-%m-%Y')

    
    
    date = d.today()-timedelta(1)
    current_date2 = date.strftime('%d-%m-%Y')

    date = d.today()-timedelta(2)
    current_date3 = date.strftime('%d-%m-%Y')

    date = d.today()
    current_date4 = date.strftime('%d-%m-%Y')
   
    date = d.today()
    current_date6 = date.strftime('%Y%m%d')

    current_date = d.today()-timedelta(1)

    date = d.today()
    current_year = date.strftime('%Y')

    date = d.today()
    current_month = date.strftime('%m')

    date = d.today()-timedelta(1)
    current_day = date.strftime('%d')
    
    date = d.today()-timedelta(2)
    previous_day = date.strftime('%d')
    
    date = d.today()-timedelta(1)
    previous_date = date.strftime('%Y-%m-%d')

    date = d.today()
    current_mon = date.strftime('%b')
    
    date = d.today()
    current_yr = date.strftime('%y')
    
    credentials = service_account.Credentials.from_service_account_file(key_path,scopes=["https://www.googleapis.com/auth/cloud-platform"])
    project_id = 'spicemoney-dwh'
    client = bigquery.Client(credentials=credentials, project=project_id, location='asia-south1')
   
    #fa=open('/home/sdlreco/crons/ybl_aeps/error/missing-'+str(date)+'.txt', 'w')
    #fa.close()
    
    #Specifying the path of the external file
    file_path = [str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/EmailReports/Axis_Axis-bank miis/SPICEMONEY_CDM_MIS'
                +str(current_date6)+'*.xlsx']
    
    # file_path= 'D:/axis-bank-mis-July-22.csv'

    '''
    
     #---------------------------------------------------------------------------------------------------------------------
    #Loading the CustomerCare file into the database
    #---------------------------------------------------------------------------------------------------------------------
    try:
        schema_cc_file = [
                          {'name':'agent_name','type':'STRING'},
                            {'name':'agent_mobile','type':'STRING'},
                            {'name':'agent_id','type':'STRING'},
                            {'name':'mobile_number','type':'STRING'},
                            {'name':'operator','type':'STRING'},
                            {'name':'transaction_id','type':'STRING'},
                            {'name':'date_of_transaction','type':'DATETIME'},
                            {'name':'amount','type':'FLOAT'},
                            {'name':'date_of_complaint','type':'DATETIME'},
                            {'name':'month','type':'STRING'},
                            {'name':'aggregator','type':'STRING'},
                            {'name':'remarks_mode','type':'STRING'},
                            {'name':'ra_remarks','type':'STRING'},
                            {'name':'final_status','type':'STRING'}   


        ]


        header_cc_file =['agent_name',
                        'agent_mobile',
                        'agent_id',
                        'mobile_number',
                        'operator',
                        'transactionid',
                         'date_of_transaction',
                                'amount',
                                'date_of_complaint',
                                'month',
                                'aggregator',
                                'remarks_mode',
                                'ra_remarks',
                                'final_status']

        list1_cc_file  =['agent_name',
                        'agent_mobile',
                        'agent_id',
                        'mobile_number',
                        'operator',
                        'transactionid',
                                'month',
                                'aggregator',
                                'remarks_mode',
                                'ra_remarks',
                                'final_status'       
        ]

        list2_cc_file  =[
            'amount'  
        ]


        df_cc_file=pd.read_excel('Recharge data till 2nd sep-22',sheet_name='data',skiprows=1,names=header_cc_file ,storage_options={"token": key_path},header=None , parse_dates = (['date_of_transaction','date_of_complaint']))             


        df_cc_file[list1_cc_file]=df_njri_com[list1_cc_file].astype(str)
        df_cc_file[list2_cc_file]=df_njri_com[list2_cc_file].astype(float)

        df_cc_file.to_gbq(destination_table='sm_recon.ts_recharge_customer_care_file', project_id='spicemoney-dwh', if_exists='replace' , table_schema = schema_cc_file,credentials=credentials)
        print("Data moved to ts_recharge_customer_care_file table")

        #df.to_gbq(destination_table='prod_sm_recon.prod_recharge_njri_commission_calculate_report', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_tw_ref,credentials=credentials)
        #print("Data moved to prod_recharge_njri_commission_calculate_report table")

    except:
            print('CUSTOMER CARE file not received')
            

    #---------------------------------------------------------------------------------------------------------------------
    #Loading the CustomerCare file into the database
    #---------------------------------------------------------------------------------------------------------------------
  #try:
    
    schema_cc_file = [
                            {'name':'transaction_id','type':'STRING'},
                            {'name':'load_date','type':'DATE'}


    ]


    header_cc_file =[
                        'transaction_id',
                         'load_date']

    list1_cc_file  =[
                        'transaction_id'      
        ]

    df_cc_file=pd.read_excel('Recharge data till 2nd sep-22-trans.xlsx',sheet_name='data',skiprows=1,names=header_cc_file,header=None , parse_dates = (['load_date']))             


    df_cc_file[list1_cc_file]=df_cc_file[list1_cc_file].astype(str)
    #df_cc_file['load_date']=load_date
    df_cc_file['load_date']="2022-09-19"
    print(df_cc_file['load_date'])
    df_cc_file.to_gbq(destination_table='sm_recon.ts_recharge_customer_care_file', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_cc_file,credentials=credentials)
    print("Data moved to ts_recharge_customer_care_file table")

    #df.to_gbq(destination_table='prod_sm_recon.prod_recharge_customer_care_file', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_cc_file,credentials=credentials)
    #print("Data moved to prod_recharge_njri_commission_calculate_report table")

   # except:
    #        print('CUSTOMER CARE file not received')
   
    '''
    
    #---------------------------------------------------------------------------------------------------------------------
    #Loading NJRIRechargeCommissionReport Summary
    #---------------------------------------------------------------------------------------------------------------------

    schema_njrisummary= [{'name':'operator','type':'STRING'},
            {'name':'count','type':'FLOAT'},
            {'name':'volume','type':'FLOAT'},
            {'name':'margin','type':'FLOAT'},
            {'name':'load_date','type':'DATE'}
              
              ]
                
    #Specifying the header column            
    header_list_njrisummary = ['operator','count','volume','margin','load_date']
    
    list1_njrisummary= ['operator']
    
    list2_njrisummary=['count','volume','margin']
     
    print('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/NJRIRechargeCommissionReport/CommissionReport_'+'*.xlsx')
            # Reading data from excel to dataframe            
    df_njrisummary = pd.read_excel('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/NJRIRechargeCommissionReport/CommissionReport_'+'*.xlsx',skiprows=1,names=header_list_njrisummary,storage_options={"token": key_path},header=None)  
    
    
    df_njrisummary[list1_njrisummary]=df_njrisummary[list1_njrisummary].astype(str)
    df_njrisummary[list2_njrisummary]=df_njrisummary[list2_njrisummary].astype(float)
    df_njrisummary['load_date']=str(previous_date)
   

    df_njrisummary.to_gbq(destination_table='sm_recon.ts_recharge_njri_recharge_sales_summary', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_njrisummary,credentials=credentials)
    print("Data moved to ts_recharge_njri_recharge_sales_summary table")
    df_njrisummary.to_gbq(destination_table='prod_sm_recon.prod_recharge_njri_recharge_sales_summary', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_njrisummary,credentials=credentials)
    print("Data moved to prod_recharge_njri_recharge_sales_summary table")
    print("---------------------------------------------------------------")
    
    
    #---------------------------------------------------------------------------------------------------------------------
    #Loading Think WalletRechargesales_summaryinto the database
    #---------------------------------------------------------------------------------------------------------------------

    schema_twsummary= [{'name':'filedate','type':'DATE'},
            {'name':'biller','type':'STRING'},
            {'name':'total_txn','type':'FLOAT'},
            {'name':'total_txn_amount','type':'FLOAT'},
            {'name':'gross_sales','type':'FLOAT'},
            {'name':'total_reversals_amount','type':'FLOAT'},
            {'name':'reversals_count','type':'FLOAT'},
            {'name':'reversals_amount','type':'FLOAT'},
            {'name':'net_amount','type':'FLOAT'},
            {'name':'netsales','type':'FLOAT'},
            {'name':'percentage_margin','type':'FLOAT'},
            {'name':'margin_amt','type':'FLOAT'}
              
              ]
                
    #Specifying the header column            
    header_list_twsummary = ['filedate',
            'biller',
            'total_txn',
            'total_txn_amount',
            'gross_sales',
            'total_reversals_amount',
            'reversals_count',
            'reversals_amount',
            'net_amount',
            'netsales',
            'percentage_margin',
            'margin_amt'
            ]
    
    list1_twsummary= ['biller']
    
    list2_twsummary=['total_txn',
            'total_txn_amount',
            'gross_sales',
            'total_reversals_amount',
            'reversals_count',
            'reversals_amount',
            'net_amount',
            'netsales',
            'percentage_margin',
            'margin_amt'
         ]
    
    print('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/Think WalletRechargesales_summary/sales_summary.csv')
            # Reading data from excel to dataframe            
    df_twsummary = pd.read_csv('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/Think WalletRechargesales_summary/sales_summary.csv',skiprows=2,names=header_list_twsummary,storage_options={"token": key_path},header=None,parse_dates = (['filedate']), low_memory=False)  
    print(df_twsummary.info())
    #df_twsummary['filedate'] = pd.to_datetime(df_twsummary['filedate'])
    df_twsummary[list1_twsummary]=df_twsummary[list1_twsummary].astype(str)
    df_twsummary[list2_twsummary]=df_twsummary[list2_twsummary].astype(float)
    print(df_twsummary.info())
   

    df_twsummary.to_gbq(destination_table='sm_recon.ts_recharge_think_wallet_recharge_sales_summary', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_twsummary,credentials=credentials)
    print("Data moved to ts_recharge_think_wallet_recharge_sales_summary table")
    df_twsummary.to_gbq(destination_table='prod_sm_recon.prod_recharge_think_wallet_recharge_sales_summary', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_twsummary,credentials=credentials)
    print("Data moved to prod_recharge_think_wallet_recharge_sales_summary table")
    print("---------------------------------------------------------------")
    
    
    #---------------------------------------------------------------------------------------------------------------------
    #Loading theThink WalletRechargeLogs into the database
    #---------------------------------------------------------------------------------------------------------------------

    schema = [{'name':'mdn','type':'STRING'},
            {'name':'status','type':'STRING'},
            {'name':'amount','type':'FLOAT'},
            {'name':'amount_deducted','type':'FLOAT'},
            {'name':'rollback_amount','type':'FLOAT'},
            {'name':'txn_id','type':'STRING'},
            {'name':'client_txn_id','type':'STRING'},
            {'name':'operator_txn_id','type':'STRING'},
            {'name':'request_timestamp','type':'TIMESTAMP'},
            {'name':'response_timestamp','type':'TIMESTAMP'},
            {'name':'operator','type':'STRING'},
            {'name':'service','type':'STRING'},
            {'name':'response','type':'STRING'}
              
              ]
                
    #Specifying the header column            
    header_list = ['mdn',
                    'status',
                    'amount',
                    'amount_deducted',
                    'rollback_amount',
                    'txn_id',
                    'client_txn_id',
                    'operator_txn_id',
                    'request_timestamp',
                    'response_timestamp',
                    'operator',
                    'service',
                    'response']
    
    list1= ['mdn',
            'status',
            'txn_id',
            'client_txn_id',
            'operator_txn_id',
            'operator',
            'service',
            'response']
    list2=['amount',
            'amount_deducted',
            'rollback_amount']
    
   
    print('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/Think WalletRechargeLogs/logs.csv')
    # Reading data from excel to dataframe            
    df = pd.read_csv('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/Think WalletRechargeLogs/logs.csv',skiprows=1,names=header_list,storage_options={"token": key_path},header=None,parse_dates = (['request_timestamp','response_timestamp']), low_memory=False)  


    df['client_txn_id']=df['client_txn_id'].str.replace(r"'",'')
    df[list1]=df[list1].astype(str)
    df[list2]=df[list2].astype(float)
    df.to_gbq(destination_table='sm_recon.ts_recharge_think_wallet_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema,credentials=credentials)
    print("Data moved to ts_recharge_think_wallet_log table")
    df.to_gbq(destination_table='prod_sm_recon.prod_recharge_think_wallet_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema,credentials=credentials)
    print("Data moved to prod_recharge_think_wallet_log table")    
    print("---------------------------------------------------------------")
            
    
    #---------------------------------------------------------------------------------------------------------------------
    #Loading the NJRI Transaction Report file into the database
    #---------------------------------------------------------------------------------------------------------------------
    schema_njri = [{'name':'order_no.','type':'STRING'},
            {'name':'order_date','type':'DATE'},
            {'name':'order_time','type':'STRING'},
            {'name':'mobile_dth_no','type':'STRING'},
            {'name':'amount','type':'FLOAT'},
            {'name':'recharge_status','type':'STRING'},
            {'name':'nick_name','type':'STRING'},
            {'name':'service_name','type':'STRING'},
            {'name':'service_provider','type':'STRING'},
            {'name':'service_type','type':'STRING'},
            {'name':'reason','type':'STRING'},
            {'name':'system_ref_no','type':'STRING'},
            {'name':'operator_txn_id','type':'STRING'}
              
              ]
                
    #Specifying the header column            
    header_list_njri = ['order_no',
                'order_date',
                'order_time',
                'mobile_dth_no',
                'amount',
                'recharge_status',
                'nick_name',
                'service_name',
                'service_provider',
                'service_type',
                'reason',
                'system_ref_no',
                'operator_txn_id']
    
    list1_njri= ['order_no',
            'mobile_dth_no',
            'recharge_status',
            'nick_name',
            'service_name',
            'service_provider',
            'service_type',
            'reason',
            'system_ref_no',
            'operator_txn_id']
    
    list2_njri=['amount']
    

    print('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/NJRIRechargeTransactionReport/TransactionReport.xlsx')         
    df_njri = pd.read_excel('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/NJRIRechargeTransactionReport/TransactionReport.xlsx',skiprows=1,names=header_list_njri,storage_options={"token": key_path},header=None)             

    df_njri['order_date'] = pd.to_datetime(df_njri['order_date'])

    df_njri['order_date']=pd.to_datetime(df_njri['order_date'].dt.strftime('%d-%m-%Y'))


    df_njri['system_ref_no']=df_njri['system_ref_no'].str.replace(r"'",'')
    df_njri[list1_njri]=df_njri[list1_njri].astype(str)
    df_njri[list2_njri]=df_njri[list2_njri].astype(float)

    df_njri.to_gbq(destination_table='sm_recon.ts_recharge_njri_transaction_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_njri,credentials=credentials)
    print("Data moved to ts_recharge_njri_transaction_log table")
    df_njri.to_gbq(destination_table='prod_sm_recon.prod_recharge_njri_transaction_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_njri,credentials=credentials)
    print("Data moved to prod_recharge_njri_transaction_log table")
    print("---------------------------------------------------------------")
    

   #---------------------------------------------------------------------------------------------------------------------
   #Loading the JIO Transaction Report file into the database
   #---------------------------------------------------------------------------------------------------------------------
    

    schema_jio =[ {'name':'circle_id','type':'STRING'},
        {'name':'amount','type':'FLOAT'},
        {'name':'plan_id','type':'STRING'},
        {'name':'transaction_time','type':'FLOAT'},
        {'name':'trans_id','type':'STRING'},
        {'name':'refill_id','type':'STRING'},
        {'name':'result_code','type':'STRING'},
        {'name':'result_description','type':'STRING'},
        {'name':'source_agent_code','type':'STRING'},
        {'name':'source_agent_name','type':'STRING'},
        {'name':'payment_mode','type':'STRING'},
        {'name':'source_opening_balance','type':'FLOAT'},
        {'name':'source_closing_balance','type':'FLOAT'},
        {'name':'trans_type','type':'STRING'},
        {'name':'ci_status','type':'STRING'},
                 {'name':'real_time','type':'DATETIME'}
                ]
    
    
     #Specifying the header column            
    header_list_jio = ['circle_id',
                        'amount',
                        'plan_id',
                        'transaction_time',
                        'trans_id',
                        'refill_id',
                        'result_code',
                        'result_description',
                        'source_agent_code',
                        'source_agent_name',
                        'payment_mode',
                        'source_opening_balance',
                        'source_closing_balance',
                        'trans_type',
                        'ci_status','real_time']
    
    list1_jio= ['circle_id',
                'plan_id',
                'trans_id',
                'refill_id',
                'result_code',
                'result_description',
                'source_agent_code',
                'source_agent_name',
                'payment_mode',
                'trans_type',
                'ci_status']
    
    list2_jio=['amount',
                'source_opening_balance',
                'source_closing_balance','transaction_time']
   
    
    print('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/EmailReports/JIO_JIO/Spice Money_'+str(previous_day)+' '+ str(current_mon)+' '+str(current_yr) +'.xlsxb')

    df_jio=pd.read_excel('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/EmailReports/JIO_JIO/Spice Money_'+str(previous_day)+' '+ str(current_mon)+' '+str(current_yr) +'.xlsxb', dtype=object,storage_options={"token": key_path}, skiprows=1,names=header_list_jio,header=None)

    df_jio['real_time'] = pd.TimedeltaIndex(df_jio['transaction_time'], unit='d') + dt.datetime(1899, 12, 30)
    df_jio['real_time']=pd.to_datetime(df_jio['real_time'].dt.strftime('%Y-%m-%d %H:%M:%S'))
    df_jio[list1_jio]=df_jio[list1_jio].astype(str)
    df_jio[list2_jio]=df_jio[list2_jio].astype(float)
    df_jio['refill_id']=df_jio['refill_id'].str.replace(r"'",'')


    df_jio.to_gbq(destination_table='sm_recon.ts_recharge_jio_spice_money_log', project_id='spicemoney-dwh', if_exists='append', table_schema = schema_jio,credentials=credentials)
    print("Data moved to ts_recharge_jio_spice_money_log table")
    df_jio.to_gbq(destination_table='prod_sm_recon.prod_recharge_jio_spice_money_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_jio,credentials=credentials)
    print("Data moved to prod_recharge_jio_spice_money_log table")
    print("---------------------------------------------------------------")
       
    
     #---------------------------------------------------------------------------------------------------------------------
    #Loading the NJRI Reversal Transaction Report file into the database
    #---------------------------------------------------------------------------------------------------------------------

    schema_njri_rev = [{'name':'order_no.','type':'STRING'},
            {'name':'order_date','type':'DATETIME'},
            {'name':'mobile_dth_no','type':'STRING'},
            {'name':'amount','type':'FLOAT'},
            {'name':'service_name','type':'STRING'},
            {'name':'service_provider','type':'STRING'},
            {'name':'transaction_status','type':'STRING'},
            {'name':'system_ref_no','type':'STRING'},
            {'name':'final_status_change_date','type':'DATETIME'},
            {'name':'nick_name','type':'STRING'}       
              
              ]
                
    #Specifying the header column            
    header_list_njri_rev = ['order_no',
                'order_date',
                'mobile_dth_no',
                'amount',
                'service_name',
                'service_provider',
                'transaction_status',
                'system_ref_no',
                'final_status_change_date',
                'nick_name']
    
    list1_njri_rev= [
           'order_no',
            'mobile_dth_no',
            'service_name',
            'service_provider',
            'transaction_status',
            'system_ref_no',
            'nick_name']
    
    list2_njri_rev=['amount']
   
    
    print('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/EmailReports/NJRI_Recharge/TransactionReversalReport-283'+"-"+str(current_day) +"-"+str(current_month)+"-"+str(current_year) +"*.xlsx")
                                                                                    
    df_njri_rev = pd.read_excel('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/EmailReports/NJRI_Recharge/TransactionReversalReport-283'+"-"+str(current_day) +"-"+str(current_month)+"-"+str(current_year) +"*.xlsx",skiprows=1,names=header_list_njri_rev ,storage_options={"token": key_path},header=None,parse_dates = (['order_date','final_status_change_date']))             


    df_njri_rev [list1_njri_rev]=df_njri_rev[list1_njri_rev].astype(str)
    df_njri_rev [list2_njri_rev]=df_njri_rev[list2_njri_rev].astype(float)
    df_njri_rev['system_ref_no']=df_njri_rev['system_ref_no'].str.replace(r"'",'')


    df_njri_rev.to_gbq(destination_table='sm_recon.ts_recharge_njri_rev_transaction_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_njri_rev ,credentials=credentials)
    print("Data moved to ts_recharge_njri_rev_transaction_log table")
    df_njri_rev.to_gbq(destination_table='prod_sm_recon.prod_recharge_njri_rev_transaction_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_njri_rev,credentials=credentials)
    print("Data moved to prod_recharge_njri_rev_transaction_log table")
    print("---------------------------------------------------------------")
    
    
    #---------------------------------------------------------------------------------------------------------------------
    #Loading the ThinkWallet Refund Transaction Report file into the database
    #---------------------------------------------------------------------------------------------------------------------
    
    
    schema_tw_ref = [{'name':'mdn','type':'STRING'},
            {'name':'status','type':'STRING'},
            {'name':'amount','type':'FLOAT'},
            {'name':'amount_deducted','type':'FLOAT'},
            {'name':'rollback_amount','type':'FLOAT'},
            {'name':'txn_id','type':'STRING'},
            {'name':'client_txn_id','type':'STRING'},
            {'name':'operator_txn_id','type':'STRING'},
            {'name':'request_timestamp','type':'TIMESTAMP'},
            {'name':'response_timestamp','type':'TIMESTAMP'},
            {'name':'operator','type':'STRING'},
            {'name':'service','type':'STRING'}
              
              ]
                
    #Specifying the header column            
    header_list_tw_ref = ['mdn',
                    'status',
                    'amount',
                    'amount_deducted',
                    'rollback_amount',
                    'txn_id',
                    'client_txn_id',
                    'operator_txn_id',
                    'request_timestamp',
                    'response_timestamp',
                    'operator',
                    'service']
    
    list1_tw_ref= ['mdn',
            'status',
            'txn_id',
            'client_txn_id',
            'operator_txn_id',
            'operator',
            'service']
    list2_tw_ref=['amount',
            'amount_deducted',
            'rollback_amount']
  
   
    print('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/Think WalletRechargeRefund file/refund.csv')
    df_tw_ref = pd.read_csv('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/Think WalletRechargeRefund file/refund.csv',skiprows=1,names=header_list_tw_ref ,storage_options={"token": key_path},header=None,
                                    parse_dates = (['request_timestamp','response_timestamp']), low_memory=False)             


    df_tw_ref[list1_tw_ref]=df_tw_ref[list1_tw_ref].astype(str)
    df_tw_ref[list2_tw_ref]=df_tw_ref[list2_tw_ref].astype(float)
    df_tw_ref['client_txn_id']=df_tw_ref['client_txn_id'].str.replace(r"'",'')


    df_tw_ref.to_gbq(destination_table='sm_recon.ts_recharge_think_wallet_refund_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_tw_ref,credentials=credentials)
    print("Data moved to ts_recharge_think_wallet_refund_log table")

    df_tw_ref.to_gbq(destination_table='prod_sm_recon.prod_recharge_think_wallet_refund_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_tw_ref,credentials=credentials)
    print("Data moved to prod_recharge_think_wallet_refund_log table")
    print("---------------------------------------------------------------")
    
    
     #---------------------------------------------------------------------------------------------------------------------
    #Loading the ReversedCommissionReport  ( NJRI) into the database
    #---------------------------------------------------------------------------------------------------------------------
    schema_njri_revcom = [{'name':'order_no','type':'STRING'},
                        {'name':'order_date','type':'DATETIME'},
                        {'name':'service_name','type':'STRING'},
                        {'name':'service_provider','type':'STRING'},
                        {'name':'mobile_dth_datacard','type':'STRING'},
                        {'name':'gst_type','type':'STRING'},
                        {'name':'recharge_type','type':'STRING'},
                        {'name':'system_reference_no','type':'STRING'},
                        {'name':'nick_name','type':'STRING'},
                        {'name':'recharge_amount','type':'FLOAT'},
                        {'name':'commission_percentage','type':'FLOAT'},
                        {'name':'commission_amount','type':'FLOAT'},
                        {'name':'reversal_date','type':'DATETIME'}
              ]
                
    #Specifying the header column            
    header_list_njri_revcom = ['order_no',
                            'order_date',
                            'service_name',
                            'service_provider',
                            'mobile_dth_datacard',
                            'gst_type',
                            'recharge_type',
                            'system_reference_no',
                            'nick_name',
                            'recharge_amount',
                            'commission_percentage',
                            'commission_amount',
                             'reversal_date']
    
    list1_njri_revcom= ['order_no',
                     'service_name',
                    'service_provider',
                    'mobile_dth_datacard',
                    'gst_type',
                    'recharge_type',
                    'system_reference_no',
                    'nick_name']
    list2_njri_revcom=['recharge_amount',
                    'commission_percentage',
                    'commission_amount']
    
    print('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/EmailReports/NJRI_Recharge/ReversedCommissionReport_*.xlsx')
    df_njri_revcom = pd.read_excel('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/EmailReports/NJRI_Recharge/ReversedCommissionReport_*.xlsx',skiprows=1,names=header_list_njri_revcom ,storage_options={"token": key_path},
                                         header=None,sheet_name='Report', parse_dates = (['order_date','reversal_date']))             

    df_njri_revcom[list1_njri_revcom]=df_njri_revcom[list1_njri_revcom].astype(str)
    df_njri_revcom[list2_njri_revcom]=df_njri_revcom[list2_njri_revcom].astype(float)
    df_njri_revcom['system_reference_no']=df_njri_revcom['system_reference_no'].str.replace(r"'",'')
    df_njri_revcom.to_gbq(destination_table='sm_recon.ts_recharge_njri_reversed_commission_report', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_njri_revcom,credentials=credentials)
    print("Data moved to ts_recharge_njri_reversed_commission_report table")

    df_njri_revcom.to_gbq(destination_table='prod_sm_recon.prod_recharge_njri_reversed_commission_report', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_njri_revcom,credentials=credentials)
    print("Data moved to prod_recharge_njri_reversed_commission_report table")
    print("---------------------------------------------------------------")
    
    
     #---------------------------------------------------------------------------------------------------------------------
    #Loading the CommissionCalculateReport  ( NJRI) into the database
    #---------------------------------------------------------------------------------------------------------------------
    schema_njri_com = [{'name':'order_no','type':'STRING'},
                        {'name':'order_date','type':'DATETIME'},
                        {'name':'service_name','type':'STRING'},
                        {'name':'service_provider','type':'STRING'},
                        {'name':'mobile_dth_datacard','type':'STRING'},
                        {'name':'gst_type','type':'STRING'},
                        {'name':'recharge_type','type':'STRING'},
                        {'name':'system_reference_no','type':'STRING'},
                        {'name':'nick_name','type':'STRING'},
                        {'name':'recharge_amount','type':'FLOAT'},
                        {'name':'commission_percentage','type':'FLOAT'},
                        {'name':'commission_amount','type':'FLOAT'}
        
    ]
    
    header_list_njri_com =['order_no',
                            'order_date',
                            'service_name',
                            'service_provider',
                            'mobile_dth_datacard',
                            'gst_type',
                            'recharge_type',
                            'system_reference_no',
                            'nick_name',
                            'recharge_amount',
                            'commission_percentage',
                            'commission_amount']
    
    
    list1_njri_com= ['order_no',
                     'service_name',
                    'service_provider',
                    'mobile_dth_datacard',
                    'gst_type',
                    'recharge_type',
                    'system_reference_no',
                    'nick_name']
    
    
    list2_njri_com=['recharge_amount',
                    'commission_percentage',
                    'commission_amount']
   
    print('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/EmailReports/NJRI_Recharge/CommissionCalculateReport_*.xlsx')
    df_njri_com = pd.read_excel('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/EmailReports/NJRI_Recharge/CommissionCalculateReport_*.xlsx',sheet_name='Report',skiprows=1,names=header_list_njri_com ,storage_options={"token": key_path},
                                     header=None , parse_dates = (['order_date']))             


    df_njri_com[list1_njri_com]=df_njri_com[list1_njri_com].astype(str)
    df_njri_com[list2_njri_com]=df_njri_com[list2_njri_com].astype(float)
    df_njri_com['system_reference_no']=df_njri_com['system_reference_no'].str.replace(r"'",'')

    df_njri_com.to_gbq(destination_table='sm_recon.ts_recharge_njri_commission_calculate_report', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_njri_com,credentials=credentials)
    print("Data moved to ts_recharge_njri_commission_calculate_report table")

    df_njri_com.to_gbq(destination_table='prod_sm_recon.prod_recharge_njri_commission_calculate_report', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_njri_com,credentials=credentials)
    print("Data moved to prod_recharge_njri_commission_calculate_report table")
    print("---------------------------------------------------------------")
   
   ################################################################################################################
    #1 & 2 Reconciliation-Spice Agent Wallet Vs TW,NJRI , JIO Transaction logs: -		
    ###############################################################################################################
    
    print("Loading of the ts_recharge_wallet_vs_tw_njri_jio_sdlwise_log started")
    sql_query_sdl_wise='''select Transaction_Id as SDL_TRANS_ID,trans_ref_no as SDL_Trans_Ref_No,SDL_COMMENTS,Trans_Status,SDL_STATUS, SDL_Refund_Amount,Transaction_Date,SDL_Trans_Amount from 
        (select Transaction_Id,
        case 
                when SDL_Refund_Amount is null then 'SUCCESS' else 'REFUND' end as SDL_STATUS,
                SDL_Refund_Amount,Transaction_Date,SDL_Trans_Amount,Trans_Status,trans_ref_no,
        case 
                when  substr(Transaction_Id,0,2)='TW' then 'THINKWALNUT'
                 when  substr(Transaction_Id,0,4)='NJRI' then 'NJRI'
                 when  substr(Transaction_Id,0,3)='JIO' then 'JIO'
                 end as SDL_COMMENTS       
        from
          
          (
          select trans_id as Transaction_Id,sum(trans_amt) as SDL_Trans_Amount,trans_status as Trans_Status,trans_date as Transaction_Date 
          from prod_dwh.wallet_trans  where date(trans_date)=@date and comments in ('Recharge_Mobile')
        group by Transaction_Id,trans_status,trans_date 
        ) as spice_wallet_output
        LEFT OUTER JOIN
        (  select refund_type,trans_id,refund_amt as SDL_Refund_Amount,refund_date,a.client_id,wallet_id,opening_bal,closing_bal,device_no,trans_date,trans_ref_no , comments, c.client_wallet_id as DistributorWalletId,
                from prod_dwh.client_refund a, prod_dwh.distributor_retailer b , prod_dwh.client_wallet c where
                a.client_id= b.retailer_id and b.distributor_id= c.client_id and refund_type="Recharge" 
                and date(trans_date)=@date
        ) as refund_report_output
        ON spice_wallet_output.Transaction_Id=refund_report_output.trans_id
        )
        '''
    
    job_config_sdl_wise = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_wallet_vs_tw_njri_jio_sdlwise_log', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
    
    job_config_sdl_wise_2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_wallet_vs_tw_njri_jio_sdlwise_log', write_disposition='WRITE_APPEND' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
    
    query_job_sdl_wise = client.query(sql_query_sdl_wise, job_config=job_config_sdl_wise)
    query_job_sdl_wise = client.query(sql_query_sdl_wise, job_config=job_config_sdl_wise_2)

    results = query_job_sdl_wise.result()
    print("moved to ts_recharge_wallet_vs_tw_njri_jio_sdlwise_log")
    print("moved to prod_recharge_wallet_vs_tw_njri_jio_sdlwise_log")
    
    
    print("Loading of the ts_recharge_wallet_vs_tw_njri_jio_agg_wise_log started")
    sql_query_agg_wise='''select AGG_TRANS_ID, AGG_Trans_Ref_No,  AGG_NAME , 
       case when REF_AGG_AMOUNT is not null then 'REFUND' else AGG_TRANS_STATUS end as AGG_STATUS,AGG_DATE,
     AGG_AMOUNT,REF_AGG_AMOUNT
    from 
     (select client_txn_id as AGG_TRANS_ID,txn_id as AGG_Trans_Ref_No, case 
                    when  substr(client_txn_id,0,2)='TW' then 'TW'
                     end as AGG_NAME ,status as AGG_TRANS_STATUS,date(request_timestamp) as AGG_DATE,sum(amount) as AGG_AMOUNT from `sm_recon.ts_recharge_think_wallet_log`
              where date(request_timestamp)=@date
              group by status,client_txn_id,date(request_timestamp),txn_id
              UNION ALL
              select system_ref_no,order_no,
              case 
                     when  substr(system_ref_no,0,4)='NJRI' then 'NJRI'
                     end as AGG_NAME,
                     recharge_status,order_date,sum(amount)
                      from `sm_recon.ts_recharge_njri_transaction_log`
              where date(order_date)=@date 
              group by system_ref_no,order_date,recharge_status,order_no
              UNION ALL
              select refill_id,trans_id,
              case 
                     when  substr(refill_id,0,3)='JIO' then 'JIO'
                     end as AGG_NAME ,result_description,date(real_time),sum(amount)
               from `sm_recon.ts_recharge_jio_spice_money_log`
              where date(real_time)=@date
                group by refill_id,result_description,date(real_time),trans_id
     )as agg_txn_output
     LEFT OUTER JOIN
     (
         select client_txn_id as REF_AGG_TRANS_ID,txn_id as REF_AGG_Trans_Ref_No, 
      case 
                    when  substr(client_txn_id,0,2)='TW' then 'TW'
                     end as REF_AGG_NAME ,status as REF_AGG_TRANS_STATUS,date(response_timestamp) as REF_AGG_DATE,sum(amount) as REF_AGG_AMOUNT from `sm_recon.ts_recharge_think_wallet_refund_log`
              where date(response_timestamp)=@date
              group by status,client_txn_id,date(response_timestamp),txn_id
              UNION ALL
              select system_ref_no,order_no,
              case 
                     when  substr(system_ref_no,0,4)='NJRI' then 'NJRI'
                     end as AGG_NAME,
                     transaction_status,date(final_status_change_date),sum(amount)
                      from `sm_recon.ts_recharge_njri_rev_transaction_log`
              where date(final_status_change_date)=@date 
              group by system_ref_no,date(final_status_change_date),transaction_status,order_no

     ) as agg_rev_output
    ON agg_txn_output.AGG_TRANS_ID=agg_rev_output.REF_AGG_TRANS_ID
        '''
    
    job_config_agg_wise = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_wallet_vs_tw_njri_jio_agg_wise_log', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
    
    job_config_agg_wise_2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_wallet_vs_tw_njri_jio_agg_wise_log', write_disposition='WRITE_APPEND' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
    
    query_job_agg_wise = client.query(sql_query_agg_wise, job_config=job_config_agg_wise)
    query_job_agg_wise = client.query(sql_query_agg_wise, job_config=job_config_agg_wise_2)

    results = query_job_agg_wise.result()
    print("moved to ts_recharge_wallet_vs_tw_njri_jio_agg_wise_log")
    print("moved to prod_recharge_wallet_vs_tw_njri_jio_agg_wise_log")
    
    
    print("Loading of the ts_recharge_wallet_vs_tw_njri_jio_sdl_wise started")
    sql_query_sdl_agg='''
           select  SDL_TRANS_ID,SDL_Trans_Ref_No,SDL_COMMENTS,Trans_Status,SDL_STATUS, SDL_Refund_Amount,Transaction_Date,SDL_Trans_Amount,AGG_NAME,AGG_AMOUNT,AGG_STATUS ,(SDL_Trans_Amount-AGG_AMOUNT) as Diff from
    (
    select SDL_TRANS_ID,SDL_Trans_Ref_No,SDL_COMMENTS,Trans_Status,SDL_STATUS, SDL_Refund_Amount,Transaction_Date,coalesce(SDL_Trans_Amount,0) as SDL_Trans_Amount,AGG_NAME,coalesce(AGG_AMOUNT,0) as AGG_AMOUNT,AGG_STATUS from
    (select SDL_TRANS_ID,SDL_Trans_Ref_No,SDL_COMMENTS,Trans_Status,SDL_STATUS, SDL_Refund_Amount,Transaction_Date,SDL_Trans_Amount from 
            `sm_recon.ts_recharge_wallet_vs_tw_njri_jio_sdlwise_log` where date(Transaction_Date)=@date
    ) as sdl_wise_output
    LEFT OUTER JOIN
    (
      select AGG_TRANS_ID, AGG_Trans_Ref_No,  AGG_NAME , AGG_STATUS,AGG_DATE,
     AGG_AMOUNT,REF_AGG_AMOUNT from `sm_recon.ts_recharge_wallet_vs_tw_njri_jio_agg_wise_log` where date(AGG_DATE)=@date
    ) as agg_wise_output
    ON SDL_TRANS_ID=AGG_TRANS_ID
    )
        '''
    
    job_config_sdl_agg = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_wallet_vs_tw_njri_jio_sdl_wise', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
    
    job_config_sdl_agg_2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_wallet_vs_tw_njri_jio_sdl_wise', write_disposition='WRITE_APPEND' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
    
    query_job_sdl_agg = client.query(sql_query_sdl_agg, job_config=job_config_sdl_agg)
    query_job_sdl_agg = client.query(sql_query_sdl_agg, job_config=job_config_sdl_agg_2)

    results = query_job_agg_wise.result()
    print("moved to ts_recharge_wallet_vs_tw_njri_jio_sdl_wise")
    print("moved to prod_recharge_wallet_vs_tw_njri_jio_sdl_wise")
    
    
    print("Loading of the ts_recharge_wallet_vs_tw_njri_jio_agg_wise started")
    sql_query_agg_sdl='''
        select TRANS_ID, TRANS_REF_NO,  AGGREGATOR , AGG_STATUS,AGG_DATE, AGG_AMOUNT ,SDL_COMMENTS,SDL_STATUS,SDL_Trans_Amount,(AGG_AMOUNT-SDL_Trans_Amount) as Diff from
        (select AGG_TRANS_ID as TRANS_ID, AGG_Trans_Ref_No as TRANS_REF_NO,  AGG_NAME as AGGREGATOR , AGG_STATUS,AGG_DATE,coalesce(AGG_AMOUNT,0) as AGG_AMOUNT ,SDL_COMMENTS,SDL_STATUS,coalesce(SDL_Trans_Amount,0) as SDL_Trans_Amount from
        (
          select AGG_TRANS_ID, AGG_Trans_Ref_No,  AGG_NAME , AGG_STATUS,AGG_DATE,
         AGG_AMOUNT,REF_AGG_AMOUNT from sm_recon.ts_recharge_wallet_vs_tw_njri_jio_agg_wise_log where date(AGG_DATE)=@date
        ) as agg_wise_output
        LEFT OUTER JOIN
        (
        select SDL_TRANS_ID,SDL_Trans_Ref_No,SDL_COMMENTS,Trans_Status,SDL_STATUS, SDL_Refund_Amount,Transaction_Date,SDL_Trans_Amount from 
                sm_recon.ts_recharge_wallet_vs_tw_njri_jio_sdlwise_log where date(Transaction_Date)=@date
        ) as sdl_wise_output

        ON SDL_TRANS_ID=AGG_TRANS_ID
        )
        '''
    
    job_config_agg_sdl = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_wallet_vs_tw_njri_jio_agg_wise', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
    
    job_config_agg_sdl_2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_wallet_vs_tw_njri_jio_agg_wise', write_disposition='WRITE_APPEND' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
    
    query_job_agg_sdl = client.query(sql_query_agg_sdl, job_config=job_config_agg_sdl)
    query_job_agg_sdl = client.query(sql_query_agg_sdl, job_config=job_config_agg_sdl_2)

    results = query_job_agg_wise.result()
    print("moved to ts_recharge_wallet_vs_tw_njri_jio_agg_wise")
    print("moved to prod_recharge_wallet_vs_tw_njri_jio_agg_wise")
    print("---------------------------------------------------------------")
    
    
    ################################################################################################################
    #5-6 Commission Validation query
    ###############################################################################################################
    
      
    print("Loading of the ts_recharge_ftr_vs_njri_tw_commission_validation started")
    sql_query='''select TRANSFER_DATE,CLIENT_ID,round(AMOUNT_TRANSFERRED,3) as AMOUNT_TRANSFERRED ,COMMENTS,FTR_UNIQUE_IDENTIFICATION_NO,TRANS_TYPE,device_type,OPERATOR_TITLE,AGG_ID,round(COMMISSION_AMOUNT,3) as COMMISSION_AMOUNT,AGG_AMOUNT,Difference_AmntTransfer_AggComAmount ,ACTUAL_COMMISSION as Actual_Commission,round(Diff_Cal_Vs_Actual_Commission,3) as Diff_Cal_Vs_Actual_Commission,ACTUAL_COMMISSION_PERCENTAGE as APPLICABLE_COMMISSION_PERCENTAGE,
        Credited_Commission_Percentage,round(Diff_Cal_Vs_Actual_Charges_Percentage,3) as Diff_Cal_Vs_Actual_Charges_Percentage,
        round(AGG_COMMISSION_PERCENTAGE,3) as AGG_COMMISSION_PERCENTAGE from 


        (select TRANSFER_DATE,CLIENT_ID,AMOUNT_TRANSFERRED,COMMENTS,FTR_UNIQUE_IDENTIFICATION_NO,TRANS_TYPE,device_type,OPERATOR_TITLE,AGG_ID,COMMISSION_AMOUNT,AGG_AMOUNT,round(DIFFERENCE,3) as Difference_AmntTransfer_AggComAmount,Calculated_Commission,round(ACTUAL_COMMISSION,3) as Actual_Commission,
        AMOUNT_TRANSFERRED-ACTUAL_COMMISSION as Diff_Cal_Vs_Actual_Commission,ACTUAL_COMMISSION_PERCENTAGE,
        round(Credited_Commission_Percentage,3) as Credited_Commission_Percentage,
        (Credited_Commission_Percentage-ACTUAL_COMMISSION_PERCENTAGE) as Diff_Cal_Vs_Actual_Charges_Percentage,
        AGG_COMMISSION_PERCENTAGE from 
        (
        select TRANSFER_DATE,CLIENT_ID,AMOUNT_TRANSFERRED,COMMENTS,FTR_UNIQUE_IDENTIFICATION_NO,TRANS_TYPE,device_type,
        OPERATOR_TITLE,AGG_ID,COMMISSION_AMOUNT,round(AGG_AMOUNT,3) as AGG_AMOUNT,
        AMOUNT_TRANSFERRED-COMMISSION_AMOUNT as DIFFERENCE,
        AMOUNT_TRANSFERRED/AGG_AMOUNT as Calculated_Commission,
        (AGG_AMOUNT*ACTUAL_COMMISSION_PERCENTAGE)/100 as  ACTUAL_COMMISSION,
        (AMOUNT_TRANSFERRED/AGG_AMOUNT)*100 as Credited_Commission_Percentage,
        ACTUAL_COMMISSION_PERCENTAGE, 
        (COMMISSION_AMOUNT/AGG_AMOUNT)*100 as AGG_COMMISSION_PERCENTAGE from 
        (
        select TRANSFER_DATE,CLIENT_ID,AMOUNT_TRANSFERRED,COMMENTS,FTR_UNIQUE_IDENTIFICATION_NO,TRANS_TYPE,device_type,
        OPERATOR_TITLE,AGG_ID,COMMISSION_AMOUNT,AGG_AMOUNT,AMOUNT_TRANSFERRED-COMMISSION_AMOUNT as DIFFERENCE,
        AMOUNT_TRANSFERRED/AGG_AMOUNT as Calculated_Commission,
        CASE
          when OPERATOR_TITLE IN ('AIRTEL','RELIANCE_JIO') then 0.50
          when device_type="Mobile" and AGG_AMOUNT <200 AND OPERATOR_TITLE NOT IN ('AIRTEL','RELIANCE_JIO') then 0.75
          when device_type="Mobile" and AGG_AMOUNT >=200 AND OPERATOR_TITLE NOT IN ('AIRTEL','RELIANCE_JIO') then 2.00
          when device_type="DTH" then 1.50
          end as ACTUAL_COMMISSION_PERCENTAGE
         from 
        (select TRANSFER_DATE,CLIENT_ID,AMOUNT_TRANSFERRED,COMMENTS,FTR_UNIQUE_IDENTIFICATION_NO,TRANS_TYPE,device_type,
        OPERATOR_TITLE,AGG_ID,
        CASE
          when substr(FTR_UNIQUE_IDENTIFICATION_NO,0,2)='TW' then TW_Commission_Amount
          when substr(FTR_UNIQUE_IDENTIFICATION_NO,0,4)='NJRI' then NJRI_Commission_Amount
          end as COMMISSION_AMOUNT,
        CASE
          when substr(FTR_UNIQUE_IDENTIFICATION_NO,0,2)='TW' then TW_Amount
          when substr(FTR_UNIQUE_IDENTIFICATION_NO,0,4)='NJRI' then NJRI_Amount
          end as AGG_AMOUNT
        from
        (select date(t1.transfer_date) as TRANSFER_DATE,
        t2.retailer_id AS CLIENT_ID,
        sum(t1.amount_transferred) as AMOUNT_TRANSFERRED,
        t1.comments as COMMENTS,t1.unique_identification_no as FTR_UNIQUE_IDENTIFICATION_NO,
        t1.trans_type as TRANS_TYPE,
        case 
                when  substr(t1.unique_identification_no,0,2)='TW' then 'THINKWALNUT'
                 when  substr(t1.unique_identification_no,0,4)='NJRI' then 'NJRI'
                 when  substr(t1.unique_identification_no,0,3)='JIO' then 'JIO'
                 end as AGG_ID       
        FROM spicemoney-dwh.prod_dwh.cme_wallet as t1
                JOIN spicemoney-dwh.prod_dwh.client_details as t2 ON t1.retailer_wallet_id=t2.ret_wallet_id
                where t1.comments IN ('IRCTC Recharge Discount','RECHARGE-Discount-Mobile') and 
                DATE(t1.transfer_date) = @date
                GROUP BY t1.UNIQUE_IDENTIFICATION_NO,CLIENT_ID,COMMENTS,TRANS_TYPE,TRANSFER_DATE,AGG_ID
        ) as FTR_revoke_output
        LEFT OUTER JOIN
        (
         select t1.trans_id,t1.device_type,t1.operator_id,t2.name as OPERATOR_TITLE from prod_dwh.recharge t1, `prod_dwh.operator` t2
         where t1.operator_id=t2.operator_id
        ) as recharge_output
        ON recharge_output.trans_id=FTR_UNIQUE_IDENTIFICATION_NO
        LEFT OUTER JOIN
        (
          select commission_amount as NJRI_Commission_Amount,recharge_amount as NJRI_Amount,system_reference_no from `sm_recon.ts_recharge_njri_commission_calculate_report`
          where date(order_date)=@date
        ) as njri_commission_output
        ON njri_commission_output.system_reference_no=FTR_UNIQUE_IDENTIFICATION_NO
        LEFT OUTER JOIN
        (
          select amount as TW_Amount,amount_deducted,client_txn_id,amount-amount_deducted as TW_Commission_Amount from 
          `sm_recon.ts_recharge_think_wallet_log` where status in ('Success','Pending','Rollback') and 
          date(request_timestamp)=@date
        ) as tw_commision_output
        ON tw_commision_output.client_txn_id=FTR_UNIQUE_IDENTIFICATION_NO
        )
        )
        )
        )

    '''
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_ftr_vs_njri_tw_commission_validation', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_ftr_vs_njri_tw_commission_validation', write_disposition='WRITE_APPEND' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    query_job = client.query(sql_query, job_config=job_config2)

    results = query_job.result()
    print("Loading of ts_recharge_ftr_vs_njri_tw_commission_validation completed")
    print("Loading of prod_recharge_ftr_vs_njri_tw_commission_validation completed")
    print("---------------------------------------------------------------")
    
     ################################################################################################################
    #5-6 Revoke Commission Validation query
    ###############################################################################################################
    print("Loading of the ts_recharge_ftr_vs_njri_tw_revoke_commission_validation started")
    
    sql_query='''
            select TRANSFER_DATE,CLIENT_ID,AMOUNT_TRANSFERRED,COMMENTS,FTR_UNIQUE_IDENTIFICATION_NO,TRANS_TYPE,device_type,OPERATOR_TITLE,AGG_ID,REVOKE_COMMISSION_AMOUNT,AGG_AMOUNT,Difference_AmntTransfer_ComAmount as Difference_AmntTransfer_AggComAmount,round(ACTUAL_COMMISSION,3) as Actual_Commission,round(Diff_Cal_Vs_Actual_Commission,3) as Diff_Cal_Vs_Actual_Commission,ACTUAL_COMMISSION_PERCENTAGE as APPLICABLE_COMMISSION_PERCENTAGE,
            Cal_Commission_Percentage as Debited_Commission_Percentage,round(Diff_Cal_Vs_Actual_Charges_Percentage,3) as Diff_Cal_Vs_Actual_Charges_Percentage,
            round(AGG_REVOKE_COMMISSION_PERCENTAGE,3) as AGG_REVOKE_COMMISSION_PERCENTAGE from 


            (select TRANSFER_DATE,CLIENT_ID,AMOUNT_TRANSFERRED,COMMENTS,FTR_UNIQUE_IDENTIFICATION_NO,TRANS_TYPE,device_type,OPERATOR_TITLE,AGG_ID,REVOKE_COMMISSION_AMOUNT,AGG_AMOUNT,round(DIFFERENCE,3) as Difference_AmntTransfer_ComAmount,Calculated_Commission,ACTUAL_COMMISSION as Actual_Commission,
            AMOUNT_TRANSFERRED-ACTUAL_COMMISSION as Diff_Cal_Vs_Actual_Commission,ACTUAL_COMMISSION_PERCENTAGE,
            round(Cal_Commission_Percentage,3) as Cal_Commission_Percentage,
            (ACTUAL_COMMISSION_PERCENTAGE-Cal_Commission_Percentage) as Diff_Cal_Vs_Actual_Charges_Percentage,
            AGG_REVOKE_COMMISSION_PERCENTAGE from 
            (
            select TRANSFER_DATE,CLIENT_ID,round(AMOUNT_TRANSFERRED,3) as AMOUNT_TRANSFERRED,COMMENTS,FTR_UNIQUE_IDENTIFICATION_NO,TRANS_TYPE,device_type,OPERATOR_TITLE,AGG_ID,round(REVOKE_COMMISSION_AMOUNT,3) as REVOKE_COMMISSION_AMOUNT,round(AGG_AMOUNT,3) as AGG_AMOUNT,
            AMOUNT_TRANSFERRED-REVOKE_COMMISSION_AMOUNT as DIFFERENCE,
            AMOUNT_TRANSFERRED/AGG_AMOUNT as Calculated_Commission,
            (AGG_AMOUNT*ACTUAL_COMMISSION_PERCENTAGE)/100 as  ACTUAL_COMMISSION,
            (AMOUNT_TRANSFERRED/AGG_AMOUNT)*100 as Cal_Commission_Percentage,
            ACTUAL_COMMISSION_PERCENTAGE, 
            (REVOKE_COMMISSION_AMOUNT/AGG_AMOUNT)*100 as AGG_REVOKE_COMMISSION_PERCENTAGE from 
            (
            select TRANSFER_DATE,CLIENT_ID,AMOUNT_TRANSFERRED,COMMENTS,FTR_UNIQUE_IDENTIFICATION_NO,TRANS_TYPE,device_type,OPERATOR_TITLE,AGG_ID,REVOKE_COMMISSION_AMOUNT,AGG_AMOUNT,AMOUNT_TRANSFERRED-REVOKE_COMMISSION_AMOUNT as DIFFERENCE,
            AMOUNT_TRANSFERRED/AGG_AMOUNT as Calculated_Commission,
            CASE
              when OPERATOR_TITLE IN ('AIRTEL','RELIANCE_JIO') then 0.50
              when device_type="Mobile" and AGG_AMOUNT <200 AND OPERATOR_TITLE NOT IN ('AIRTEL','RELIANCE_JIO') then 0.75
              when device_type="Mobile" and AGG_AMOUNT >=200 AND OPERATOR_TITLE NOT IN ('AIRTEL','RELIANCE_JIO') then 2.00
              when device_type="DTH" then 1.50
              end as ACTUAL_COMMISSION_PERCENTAGE
             from 
            (select TRANSFER_DATE,CLIENT_ID,AMOUNT_TRANSFERRED,COMMENTS,FTR_UNIQUE_IDENTIFICATION_NO,TRANS_TYPE,device_type,OPERATOR_TITLE,AGG_ID,
            CASE
              when substr(FTR_UNIQUE_IDENTIFICATION_NO,0,2)='TW' then TW_Commission_Reversal_Amount
              when substr(FTR_UNIQUE_IDENTIFICATION_NO,0,4)='NJRI' then NJRI_Commission_Reversal_Amount
              end as REVOKE_COMMISSION_AMOUNT,
            CASE
              when substr(FTR_UNIQUE_IDENTIFICATION_NO,0,2)='TW' then TW_Reversal_Amount
              when substr(FTR_UNIQUE_IDENTIFICATION_NO,0,4)='NJRI' then NJRI_Reversal_Amount
              end as AGG_AMOUNT
            from
            (select date(t1.transfer_date) as TRANSFER_DATE,
            t2.retailer_id AS CLIENT_ID,
            sum(t1.amount_transferred) as AMOUNT_TRANSFERRED,
            t1.comments as COMMENTS,t1.unique_identification_no as FTR_UNIQUE_IDENTIFICATION_NO,
            t1.trans_type as TRANS_TYPE,
            case 
                    when  substr(t1.unique_identification_no,0,2)='TW' then 'THINKWALNUT'
                     when  substr(t1.unique_identification_no,0,4)='NJRI' then 'NJRI'
                     when  substr(t1.unique_identification_no,0,3)='JIO' then 'JIO'
                     end as AGG_ID       
            FROM spicemoney-dwh.prod_dwh.cme_wallet as t1
                    JOIN spicemoney-dwh.prod_dwh.client_details as t2 ON t1.retailer_wallet_id=t2.ret_wallet_id
                    where t1.comments IN ('RECHARGE-Discount-Mobile-Reversal') and 
                    DATE(t1.transfer_date) = @date
                    GROUP BY t1.UNIQUE_IDENTIFICATION_NO,CLIENT_ID,COMMENTS,TRANS_TYPE,TRANSFER_DATE,AGG_ID
            ) as FTR_revoke_output
            LEFT OUTER JOIN
            (
             select t1.trans_id,t1.device_type,t1.operator_id,t2.name as OPERATOR_TITLE from prod_dwh.recharge t1, `prod_dwh.operator` t2
             where t1.operator_id=t2.operator_id
            ) as recharge_output
            ON recharge_output.trans_id=FTR_UNIQUE_IDENTIFICATION_NO
            LEFT OUTER JOIN
            (
              select commission_amount as NJRI_Commission_Reversal_Amount,recharge_amount as NJRI_Reversal_Amount,system_reference_no 
              from `sm_recon.ts_recharge_njri_reversed_commission_report` where date(reversal_date)=@date
            ) as njri_reversed_commission_output
            ON njri_reversed_commission_output.system_reference_no=FTR_UNIQUE_IDENTIFICATION_NO
            LEFT OUTER JOIN
            (
              select amount as TW_Reversal_Amount,amount_deducted,client_txn_id,amount-rollback_amount as TW_Commission_Reversal_Amount 
              from `sm_recon.ts_recharge_think_wallet_log` where status in ('Pending','Rollback') and date(response_timestamp)=@date
            ) as tw_commision_output
            ON tw_commision_output.client_txn_id=FTR_UNIQUE_IDENTIFICATION_NO
            )
            )
            )
            )

    '''
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_ftr_vs_njri_tw_revoke_commission_validation', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_ftr_vs_njri_tw_revoke_commission_validation', write_disposition='WRITE_APPEND' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    query_job = client.query(sql_query, job_config=job_config2)

    results = query_job.result()
    print("Loading of the ts_recharge_ftr_vs_njri_tw_revoke_commission_validation completed")
    print("Loading of the prod_recharge_ftr_vs_njri_tw_revoke_commission_validation completed")
    print("---------------------------------------------------------------")
    
    ################################################################################################################
    #Recharge FTR Commission Summary
    ###############################################################################################################
    print("Loading of the ts_recharge_ftr_commission_summary started")
    sql_query='''
    select COMMENTS,TRANSFER_DATE,CLIENT_ID,AMOUNT_TRANSFERRED,device_type,OPERATOR_TITLE
        from
        (select date(t1.transfer_date) as TRANSFER_DATE,
        t2.retailer_id AS CLIENT_ID,
        sum(t1.amount_transferred) as AMOUNT_TRANSFERRED,
        t1.comments as COMMENTS,t1.unique_identification_no as FTR_UNIQUE_IDENTIFICATION_NO,
        case 
                when  substr(t1.unique_identification_no,0,2)='TW' then 'THINKWALNUT'
                 when  substr(t1.unique_identification_no,0,4)='NJRI' then 'NJRI'
                 when  substr(t1.unique_identification_no,0,3)='JIO' then 'JIO'
                 end as AGG_ID       
        FROM spicemoney-dwh.prod_dwh.cme_wallet as t1
                JOIN spicemoney-dwh.prod_dwh.client_details as t2 ON t1.retailer_wallet_id=t2.ret_wallet_id
                where t1.comments IN ('IRCTC Recharge Discount','RECHARGE-Discount-Mobile','RECHARGE-Discount-Mobile-Reversal') and 
                DATE(t1.transfer_date) = @date
                GROUP BY t1.UNIQUE_IDENTIFICATION_NO,CLIENT_ID,COMMENTS,TRANS_TYPE,TRANSFER_DATE,AGG_ID
        ) as FTR_revoke_output
        LEFT OUTER JOIN
        (
         select t1.trans_id,t1.device_type,t1.operator_id,t2.name as OPERATOR_TITLE from prod_dwh.recharge t1, `prod_dwh.operator` t2
         where t1.operator_id=t2.operator_id
        ) as recharge_output
        ON recharge_output.trans_id=FTR_UNIQUE_IDENTIFICATION_NO
           
    '''
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_ftr_commission_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_ftr_commission_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    query_job = client.query(sql_query, job_config=job_config2)

    results = query_job.result()
    print("Loading of the ts_recharge_ftr_commission_summary completed")
    print("Loading of the prod_recharge_ftr_commission_summary completed")
    print("---------------------------------------------------------------")
    
     ################################################################################################################
    #Recharge Wallet_Trans Commission Summary
    ###############################################################################################################
    print("Loading of the ts_recharge_wallet_trans_commission_summary started")
    sql_query='''
    select (@date) as recon_date,* from 
    (
    select Comments,
        case 
                when  substr(spice_trans_id,0,2)='TW' then 'THINKWALNUT'
                 when  substr(spice_trans_id,0,4)='NJRI' then 'NJRI'
                 when  substr(spice_trans_id,0,3)='JIO' then 'JIO'
                 end as Rch_Aggregator,client_id as Client_Id, device_type as Device_Type,OPERATOR_TITLE as Operator_Title,coalesce(SumOf_Trans_Amount,0) as SumOf_Trans_Amount,coalesce (SumOfB2B_RFD,0) as SumOfB2B_RFD,coalesce(SumOf_Trans_Amount,0)-coalesce (SumOfB2B_RFD,0) as NetAmount
                from
        (select comments as Comments,trans_id as spice_trans_id,sum(trans_amt) as SumOf_Trans_Amount,trans_date as Transaction_Date 
        from prod_dwh.wallet_trans  where  DATE(trans_date)= @date  and comments in ('Recharge_Mobile')
                group by trans_date ,trans_id,comments
        ) as spice_wallet_output
        LEFT OUTER JOIN
        (
         select date(recharge_date),t1.trans_id as recharge_trans_id,t1.client_id,t1.device_type,t1.operator_id,
         t2.name as OPERATOR_TITLE from prod_dwh.recharge t1, `prod_dwh.operator` t2
         where t1.operator_id=t2.operator_id 
        ) as recharge_output
        ON recharge_output.recharge_trans_id=spice_wallet_output.spice_trans_id
        LEFT OUTER JOIN
        (  
          select trans_id as Refund_Trans_id,refund_amt as SumOfB2B_RFD,refund_date,trans_date
                        from prod_dwh.client_refund a, prod_dwh.distributor_retailer b , prod_dwh.client_wallet c where
                        a.client_id= b.retailer_id and b.distributor_id= c.client_id and refund_type="Recharge" 
                        and date(refund_date)=@date
        ) as refund_result
        ON spice_wallet_output.spice_trans_id=refund_result.Refund_Trans_id
        UNION ALL
        select refund_result.Comments as Comments ,
                case 
                        when  substr(Refund_Trans_id,0,2)='TW' then 'THINKWALNUT'
                         when  substr(Refund_Trans_id,0,4)='NJRI' then 'NJRI'
                         when  substr(Refund_Trans_id,0,3)='JIO' then 'JIO'
                         end as Rch_Aggregator_Refund ,client_id as Refund_Client_Id, device_type as Refund_Device_Type ,OPERATOR_TITLE as Refund_Operator_Title,
        coalesce(SumOf_Trans_Amount,0) as SumOf_Trans_Amount,coalesce (SumOfB2B_RFD,0) as SumOfB2B_RFD,coalesce(SumOf_Trans_Amount,0)-coalesce (SumOfB2B_RFD,0) as NetAmount
                from
        (select comments as Comments,trans_id as spice_trans_id,sum(trans_amt) as SumOf_Trans_Amount,
        trans_date as Transaction_Date from prod_dwh.wallet_trans  where  DATE(trans_date)= @date 
        and comments in ('Recharge_Mobile')
                group by trans_date ,trans_id,comments
        ) as spice_wallet_output
        FULL JOIN
        (  
          select trans_id as Refund_Trans_id,refund_amt as SumOfB2B_RFD,refund_date,trans_date,comments as Comments
                        from prod_dwh.client_refund a, prod_dwh.distributor_retailer b , prod_dwh.client_wallet c where
                        a.client_id= b.retailer_id and b.distributor_id= c.client_id and refund_type="Recharge" 
                        and date(refund_date)=@date
        ) as refund_result
        ON spice_wallet_output.spice_trans_id=refund_result.Refund_Trans_id
        LEFT OUTER JOIN
        ( select date(recharge_date),t1.trans_id,t1.client_id,t1.device_type,t1.operator_id,t2.name as OPERATOR_TITLE from prod_dwh.recharge t1, `prod_dwh.operator` t2
         where t1.operator_id=t2.operator_id 
        ) as refund_recharge_output
        ON refund_recharge_output.trans_id=refund_result.Refund_Trans_id
        where  spice_wallet_output.spice_trans_id is null)
    '''
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_wallet_trans_commission_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_wallet_trans_commission_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    query_job = client.query(sql_query, job_config=job_config2)
    print("Loading of the ts_recharge_wallet_trans_commission_summary completed")
    print("Loading of the prod_recharge_wallet_trans_commission_summary completed")
    print("---------------------------------------------------------------")
    
     ################################################################################################################
    #Limit detail
    ###############################################################################################################
    print("Loading of the ts_recharge_limit_detail started")
    sql_query='''
    select (@date) as recon_date , * from 
(
    select * from
    (
          select "Recharge" as Service,Aggregator,Min_Amount_Transaction_Id,MIN_TRANSACTION_AMOUNT,Max_Amount_Transaction_Id,MAX_TRANSACTION_AMOUNT,Number_of_Transaction_Agent_Wise,Transaction_Amt_Limit_Per_Day from 
        (
         select trans_id as Min_Amount_Transaction_Id,trans_amt as MIN_TRANSACTION_AMOUNT,trans_date,Aggregator from
         (
            select *, 
            case 
                            when  substr(trans_id,0,2)='TW' then 'TW'
                            when  substr(trans_id,0,4)='NJRI' then 'NJRI'
                            when  substr(trans_id,0,3)='JIO' then 'JIO'
                            end as Aggregator    
            from prod_dwh.wallet_trans  where date(trans_date)=@date and comments in ('Recharge_Mobile')
         )
          where Aggregator='TW' and trans_amt=(select MIN(trans_amt) from prod_dwh.wallet_trans  where date(trans_date)=@date and comments in ('Recharge_Mobile') and substr(trans_id,0,2)='TW'  and trans_type='DEBIT' ) limit 1
        )as tw_t1,
        (
          select trans_id as Max_Amount_Transaction_Id,trans_amt as MAX_TRANSACTION_AMOUNT,trans_date,Max_Aggregator from
         (
         select *, 
         case 
                        when  substr(trans_id,0,2)='TW' then 'TW'
                         when  substr(trans_id,0,4)='NJRI' then 'NJRI'
                         when  substr(trans_id,0,3)='JIO' then 'JIO'
                         end as Max_Aggregator    
         from prod_dwh.wallet_trans  where date(trans_date)=@date and comments in ('Recharge_Mobile')
         )
         where Max_Aggregator='TW' and trans_amt=(select MAX(trans_amt) from prod_dwh.wallet_trans  where date(trans_date)=@date and comments in ('Recharge_Mobile') and substr(trans_id,0,2)='TW' and trans_type='DEBIT') limit 1
        )as tw_t2,
        (
           select Max(Number_of_Clients) as Number_of_Transaction_Agent_Wise from
        (
        select count(*) as Number_of_Clients from prod_dwh.wallet_trans  where date(trans_date)=@date and comments in ('Recharge_Mobile') and substr(trans_id,0,2)='TW'  and trans_type='DEBIT' group by wallet_id
        )

        )as tw_t3,
        (
         select max(amount_total) as Transaction_Amt_Limit_Per_Day from 
        (
        select sum(trans_amt) as amount_total,wallet_id from  prod_dwh.wallet_trans  where date(trans_date)=@date and comments in ('Recharge_Mobile') and substr(trans_id,0,2)='TW'  and trans_type='DEBIT' group by wallet_id
        )
        ) as tw_t4
        )
        UNION ALL
        (
          select "Recharge" as Service,Aggregator,Min_Amount_Transaction_Id,MIN_TRANSACTION_AMOUNT,Max_Amount_Transaction_Id,MAX_TRANSACTION_AMOUNT,Number_of_Transaction_Agent_Wise,Transaction_Amt_Limit_Per_Day from 
        (
         select trans_id as Min_Amount_Transaction_Id,trans_amt as MIN_TRANSACTION_AMOUNT,trans_date,Aggregator from
         (
            select *, 
            case 
                            when  substr(trans_id,0,2)='TW' then 'TW'
                            when  substr(trans_id,0,4)='NJRI' then 'NJRI'
                            when  substr(trans_id,0,3)='JIO' then 'JIO'
                            end as Aggregator    
            from prod_dwh.wallet_trans  where date(trans_date)=@date and comments in ('Recharge_Mobile')
         )
          where Aggregator='NJRI' and trans_amt=(select MIN(trans_amt) from prod_dwh.wallet_trans  where date(trans_date)=@date and comments in ('Recharge_Mobile') and substr(trans_id,0,4)='NJRI'  and trans_type='DEBIT' ) limit 1
        )as njri_t1,
        (
          select trans_id as Max_Amount_Transaction_Id,trans_amt as MAX_TRANSACTION_AMOUNT,trans_date,Max_Aggregator from
         (
         select *, 
         case 
                        when  substr(trans_id,0,2)='TW' then 'TW'
                         when  substr(trans_id,0,4)='NJRI' then 'NJRI'
                         when  substr(trans_id,0,3)='JIO' then 'JIO'
                         end as Max_Aggregator    
         from prod_dwh.wallet_trans  where date(trans_date)=@date and comments in ('Recharge_Mobile')
         )
         where Max_Aggregator='NJRI' and trans_amt=(select MAX(trans_amt) from prod_dwh.wallet_trans  where date(trans_date)=@date and comments in ('Recharge_Mobile') and substr(trans_id,0,4)='NJRI'  and trans_type='DEBIT') limit 1
        )as njri_t2,
        (
           select Max(Number_of_Clients) as Number_of_Transaction_Agent_Wise from
        (
        select count(*) as Number_of_Clients from prod_dwh.wallet_trans  where date(trans_date)=@date and comments in ('Recharge_Mobile') and substr(trans_id,0,4)='NJRI'  and trans_type='DEBIT'  group by wallet_id
        )

        )as njri_t3,
        (
         select max(amount_total) as Transaction_Amt_Limit_Per_Day from 
        (
        select sum(trans_amt) as amount_total,wallet_id from  prod_dwh.wallet_trans  where date(trans_date)=@date and comments in ('Recharge_Mobile') and substr(trans_id,0,4)='NJRI'  and trans_type='DEBIT'  group by wallet_id
        )
        ) as njri_t4
        )
        UNION ALL
        (
          select "Recharge" as Service,Aggregator,Min_Amount_Transaction_Id,MIN_TRANSACTION_AMOUNT,Max_Amount_Transaction_Id,MAX_TRANSACTION_AMOUNT,Number_of_Transaction_Agent_Wise,Transaction_Amt_Limit_Per_Day from 
        (
         select trans_id as Min_Amount_Transaction_Id,trans_amt as MIN_TRANSACTION_AMOUNT,trans_date,Aggregator from
         (
            select *, 
            case 
                            when  substr(trans_id,0,2)='TW' then 'TW'
                            when  substr(trans_id,0,4)='NJRI' then 'NJRI'
                            when  substr(trans_id,0,3)='JIO' then 'JIO'
                            end as Aggregator    
            from prod_dwh.wallet_trans  where date(trans_date)=@date and comments in ('Recharge_Mobile')
         )
          where Aggregator='JIO' and trans_amt=(select MIN(trans_amt) from prod_dwh.wallet_trans  where date(trans_date)=@date and comments in ('Recharge_Mobile') and substr(trans_id,0,3)='JIO'  and trans_type='DEBIT') limit 1
        )as njri_t1,
        (
          select trans_id as Max_Amount_Transaction_Id,trans_amt as MAX_TRANSACTION_AMOUNT,trans_date,Max_Aggregator from
         (
         select *, 
         case 
                        when  substr(trans_id,0,2)='TW' then 'TW'
                         when  substr(trans_id,0,4)='NJRI' then 'NJRI'
                         when  substr(trans_id,0,3)='JIO' then 'JIO'
                         end as Max_Aggregator    
         from prod_dwh.wallet_trans  where date(trans_date)=@date and comments in ('Recharge_Mobile')
         )
         where Max_Aggregator='JIO' and trans_amt=(select MAX(trans_amt) from prod_dwh.wallet_trans  where date(trans_date)=@date and comments in ('Recharge_Mobile') and substr(trans_id,0,3)='JIO'  and trans_type='DEBIT') limit 1
        )as njri_t2,
        (
           select Max(Number_of_Clients) as Number_of_Transaction_Agent_Wise from
        (
        select count(*) as Number_of_Clients from prod_dwh.wallet_trans  where date(trans_date)=@date and comments in ('Recharge_Mobile') and substr(trans_id,0,3)='JIO'  and trans_type='DEBIT' group by wallet_id
        )

        )as njri_t3,
        (
         select max(amount_total) as Transaction_Amt_Limit_Per_Day from 
        (
        select sum(trans_amt) as amount_total,wallet_id from  prod_dwh.wallet_trans  where date(trans_date)=@date and comments in ('Recharge_Mobile') and substr(trans_id,0,3)='JIO'  and trans_type='DEBIT' group by wallet_id
        )
        ) as njri_t4
        )
)



    '''
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_limit_detail', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_limit_detail', write_disposition='WRITE_APPEND' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    #query_job = client.query(sql_query, job_config=job_config2)
    print("Loading of the ts_recharge_limit_detail completed")
    print("Loading of the prod_recharge_limit_detail completed")
    print("---------------------------------------------------------------")
    
    
    print("--------INTERNAL reports summary generation BEGINS---------")
    ################################################################################################################
    #Internal file summary ts_recharge_wallet_txns_report_summary
    ###############################################################################################################
    print("Loading of the ts_recharge_wallet_txns_report_summary started")
    sql_query='''

        select sum(trans_amt) as SumOf_Trans_Amount,trans_type, DATE(trans_date) as Trans_Date,comments as Comments,
        case 
                        when  substr(trans_id,0,2)='TW' then 'THINKWALNUT'
                         when  substr(trans_id,0,4)='NJRI' then 'NJRI'
                         when  substr(trans_id,0,3)='JIO' then 'JIO'
                         end as Rch_Aggregator from prod_dwh.wallet_trans  where  DATE(trans_date)= @date  and comments in ('Recharge_Mobile')
                group by DATE(trans_date) ,comments,trans_type,Rch_Aggregator
      '''

    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_wallet_txns_report_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_wallet_txns_report_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    query_job = client.query(sql_query, job_config=job_config2)
    
    print("Loading of the  ts_recharge_wallet_txns_report_summary completed")
    ################################################################################################################
    #Internal file summary Refund report	summary
    ###############################################################################################################
    print("Loading of the  ts_recharge_refund_report_summary started")
    sql_query='''
        select refund_type,sum(refund_amt) as Refund_Amount,date(refund_date) as refund_date, comments,
        case 
                        when  substr(trans_id,0,2)='TW' then 'THINKWALNUT'
                         when  substr(trans_id,0,4)='NJRI' then 'NJRI'
                         when  substr(trans_id,0,3)='JIO' then 'JIO'
                         end as Aggregator
                        from prod_dwh.client_refund a, prod_dwh.distributor_retailer b , prod_dwh.client_wallet c where
                        a.client_id= b.retailer_id and b.distributor_id= c.client_id and
                        refund_type="Recharge" and date(refund_date)=@date group by refund_type,refund_date,comments,Aggregator
        '''
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_refund_report_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_refund_report_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    query_job = client.query(sql_query, job_config=job_config2)
    print("Loading of the  ts_recharge_refund_report_summary completed")
    
    ################################################################################################################
    #Internal file summary --FTR Revoke
    ###############################################################################################################
    print("Loading of the  ts_recharge_ftr_revoke_summary started")
    sql_query='''
  
        
        select DATE(transfer_date) as Transfer_Date,
                comments as Comments,
                trans_type as Trans_Type,
                sum(amount_transferred) as Amount_Transferred,
                FROM spicemoney-dwh.prod_dwh.cme_wallet
                where comments IN ('IRCTC Recharge Discount','RECHARGE-Discount-Mobile','RECHARGE-Discount-Mobile-Reversal') and 
                DATE(transfer_date) = @date
                GROUP BY comments, transfer_date,trans_type
       '''
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_ftr_revoke_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_ftr_revoke_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    query_job = client.query(sql_query, job_config=job_config2)
    print("Loading of the  ts_recharge_ftr_revoke_summary completed")


    ################################################################################################################
    #Internal file summary --Txns info report
    ###############################################################################################################
    print("Loading of the  ts_recharge_txns_info_report_summary started")
    sql_query='''
        select t1.device_type as Device_Type,date(recharge_date) as recharge_date,sum(recharge_amt) as recharge_amt,t2.name as TITLE,
        case 
                        when  substr(trans_id,0,2)='TW' then 'THINKWALNUT'
                         when  substr(trans_id,0,4)='NJRI' then 'NJRI'
                         when  substr(trans_id,0,3)='JIO' then 'JIO'
                         end as Aggregator from prod_dwh.recharge t1, `prod_dwh.operator` t2
         where t1.operator_id=t2.operator_id and date(recharge_date)=@date group by t1.device_type,date(recharge_date),t2.name,Aggregator

        '''
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_txns_info_report_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_txns_info_report_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    query_job = client.query(sql_query, job_config=job_config2)
    
    print("Loading of the  ts_recharge_txns_info_report_summary completed")

    print("--------INTERNAL reports summary generation COMPLETED!!!---------")
    print("---------------------------------------------------------------")
    

    print("--------EXTERNAL reports summary generation BEGINS!!!---------")

    ################################################################################################################
    #External file summary --NJRI Txn Report From Portal summary
    ###############################################################################################################
    print("Loading of the ts_recharge_njri_txn_report_summary started")
    sql_query='''
        select date(order_date) as order_date,sum(amount) as sum_amount,recharge_status from 
        `sm_recon.ts_recharge_njri_transaction_log` where date(order_date)=@date group 
        by order_date,recharge_status
      '''

    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_njri_txn_report_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_njri_txn_report_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    query_job = client.query(sql_query, job_config=job_config2)
    print("Loading of the  ts_recharge_njri_txn_report_summary completed")
    ################################################################################################################
    #External file summary NJRI reversal txns report from Mail
    ###############################################################################################################
    print("Loading of the  ts_recharge_njri_reversal_txn_report_summary started")
    sql_query='''
        
        select  date(final_status_change_date) as final_status_change_date,sum(amount) as amount,transaction_status
                  from `sm_recon.ts_recharge_njri_rev_transaction_log`
          where date(final_status_change_date)=@date 
          group by date(final_status_change_date),transaction_status
        
        
        '''
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_njri_reversal_txn_report_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_njri_reversal_txn_report_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    query_job = client.query(sql_query, job_config=job_config2)
    print("Loading of the  ts_recharge_njri_reversal_txn_report_summary completed")
       


    ################################################################################################################
    #External file summary TW Txns report from portal
    ###############################################################################################################
    print("Loading of the  ts_recharge_think_wallet_txn_report_summary started")
    sql_query='''
      select status as status,sum(amount) as amount,sum(amount_deducted) as amount_deducted,sum(rollback_amount) as rollback_amount,date(request_timestamp) as request_timestamp, from `sm_recon.ts_recharge_think_wallet_log`
          where date(request_timestamp)=@date
          group by status,date(request_timestamp)
       '''
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_think_wallet_txn_report_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_think_wallet_txn_report_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    query_job = client.query(sql_query, job_config=job_config2)
    print("Loading of the  ts_recharge_think_wallet_txn_report_summary completed")

    
    ################################################################################################################
    #External file summary --TW refund report from portal
    ###############################################################################################################
    print("Loading of the  ts_recharge_tw_refund_report_summary started")
    sql_query='''
       select status as status,sum(amount) as amount,sum(amount_deducted) as amount_deducted,sum(rollback_amount) as rollback_amount,date(response_timestamp) as response_timestamp from `sm_recon.ts_recharge_think_wallet_refund_log`
          where date(response_timestamp)=@date
          group by status,date(response_timestamp)
        '''
    
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_think_wallet_refund_report_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_think_wallet_refund_report_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    query_job = client.query(sql_query, job_config=job_config2)
    print("Loading of the  ts_recharge_think_wallet_refund_report_summary completed")
    
    ################################################################################################################
    #External file summary --NJRI Commission report on mail
    ###############################################################################################################
    print("Loading of the  ts_recharge_njri_commission_report_summary started")
    sql_query='''
    select date(order_date) as order_date,service_name,service_provider,gst_type,recharge_type,sum(commission_amount) as commission_amount
                  from `sm_recon.ts_recharge_njri_commission_calculate_report`
          where date(order_date)=@date 
          group by date(order_date),service_name,service_provider,gst_type,recharge_type
        
        '''
    
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_njri_commission_report_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_njri_commission_report_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    query_job = client.query(sql_query, job_config=job_config2)
    
    print("Loading of the  ts_recharge_njri_commission_report_summary completed")
    
    ################################################################################################################
    #External file summary ---NJRI Commission Reversal report on mail
    ###############################################################################################################
    print("Loading of the  ts_recharge_njri_commission_reversal_report_summary started")
    sql_query='''
        select service_name,service_provider,gst_type,recharge_type,sum(commission_amount) as commission_amount,date(reversal_date) as reversal_date
                  from `sm_recon.ts_recharge_njri_reversed_commission_report`
          where date(reversal_date)=@date
          group by date(reversal_date),service_name,service_provider,gst_type,recharge_type
        '''
    
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_njri_commission_reversal_report_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_njri_commission_reversal_report_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    query_job = client.query(sql_query, job_config=job_config2)
    print("Loading of the  ts_recharge_njri_commission_reversal_report_summary completed")
    
    ################################################################################################################
    #External file summary --JIO Txns logs on mail
    ###############################################################################################################
    print("Loading of the  ts_recharge_jio_txn_report_summary started")
    sql_query='''
       select date(real_time) as RealTime,sum(amount) as Amount,result_description
           from `sm_recon.ts_recharge_jio_spice_money_log`
          where date(real_time)=@date
            group by date(real_time),result_description
        '''
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_jio_txn_report_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_jio_txn_report_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    query_job = client.query(sql_query, job_config=job_config2)
    print("Loading of the  ts_recharge_jio_txn_report_summary completed")

    print("--------EXTERNAL reports summary generation COMPLETED!!!---------")
    print("---------------------------------------------------------------")
    
    ################################################################################################################
    #Refund reconcialliation-Based on MTD
    ###############################################################################################################
    print("Loading of the ts_recharge_spice_vs_tw_njri_cc_refund started")
    
    sql_query='''
    select *, (@date) as recon_date from 
(select Transaction_Id,Refund_Amount,Refund_Date,Client_Id,Client_Wallet_Id,Trans_Date,Trans_Ref_Num,Comments,Aggregator,
case 
when substr(Transaction_Id,0,4)='NJRI' and REF_AGG_AMOUNT is not null then "Refund received from NJRI aggregator"
when substr(Transaction_Id,0,2)='TW' and REF_AGG_AMOUNT is not null then "Refund received from TW aggregator"
--when  substr(trans_id,0,3)='JIO' and 
else AGG_TRANS_STATUS  end as AGG_STATUS,CC_status
from
(
select trans_id as Transaction_Id ,sum(refund_amt) as Refund_Amount,date(refund_date) as Refund_Date,a.client_id as Client_Id, c.client_wallet_id as Client_Wallet_Id,date(trans_date) as Trans_Date,trans_ref_no as Trans_Ref_Num , comments as Comments,
case 
                when  substr(trans_id,0,2)='TW' then 'THINKWALNUT'
                 when  substr(trans_id,0,4)='NJRI' then 'NJRI'
                 when  substr(trans_id,0,3)='JIO' then 'JIO'
                 end as Aggregator  from prod_dwh.client_refund a, prod_dwh.distributor_retailer b , prod_dwh.client_wallet c where
a.client_id= b.retailer_id and b.distributor_id= c.client_id and refund_type="Recharge" and 
(date (refund_date) >= date_trunc(date_sub((@date), interval 1 day), month) and date(refund_date) <=(@date)) 
group by trans_id,date(refund_date),client_id,client_wallet_id,date(trans_date),trans_ref_no , comments
) as wallet_output
LEFT OUTER JOIN
(
select AGG_TRANS_ID,AGG_TRANS_STATUS,AGG_AMOUNT
from 
 (select client_txn_id as AGG_TRANS_ID,txn_id as AGG_Trans_Ref_No, case 
                when  substr(client_txn_id,0,2)='TW' then 'TW'
                 end as AGG_NAME ,status as AGG_TRANS_STATUS,date(request_timestamp) as AGG_DATE,sum(amount) as AGG_AMOUNT from `sm_recon.ts_recharge_think_wallet_log`where 
(date (request_timestamp) >= date_trunc(date_sub((@date), interval 1 day), month) and date(request_timestamp) <=(@date))
          and status in ('Failed','Pending','Rollback')
          group by status,client_txn_id,date(request_timestamp),txn_id
          UNION ALL
          select system_ref_no,order_no,
          case 
                 when  substr(system_ref_no,0,4)='NJRI' then 'NJRI'
                 end as AGG_NAME,
                 recharge_status,order_date,sum(amount)
                  from `sm_recon.ts_recharge_njri_transaction_log`
          where 
          (date (order_date) >= date_trunc(date_sub((@date), interval 1 day), month) and date(order_date) <=(@date))
          and recharge_status in (
'Recharge Unsuccessful','Recharge Inprocess')
          group by system_ref_no,order_date,recharge_status,order_no
          UNION ALL
          select refill_id,trans_id,
          case 
                 when  substr(refill_id,0,3)='JIO' then 'JIO'
                 end as AGG_NAME ,result_description,date(real_time),sum(amount)
           from `sm_recon.ts_recharge_jio_spice_money_log`
          where  (date (real_time) >= date_trunc(date_sub((@date), interval 1 day), month) and date(real_time) <=(@date))
           and result_description in ('Transaction Limit Reached for Recharges')
            group by refill_id,result_description,date(real_time),trans_id
 )
)as agg_txn_output
ON wallet_output.Transaction_Id=agg_txn_output.AGG_TRANS_ID
LEFT JOIN
(
     select client_txn_id as REF_AGG_TRANS_ID,txn_id as REF_AGG_Trans_Ref_No, 
  case 
                when  substr(client_txn_id,0,2)='TW' then 'TW'
                 end as REF_AGG_NAME ,status as REF_AGG_TRANS_STATUS,date(response_timestamp) as REF_AGG_DATE,sum(amount) as REF_AGG_AMOUNT from `sm_recon.ts_recharge_think_wallet_refund_log`
          where 
          (date (response_timestamp) >= date_trunc(date_sub((@date), interval 1 day), month) and date(response_timestamp) <=(@date))
          group by status,client_txn_id,date(response_timestamp),txn_id
          UNION ALL
          select system_ref_no,order_no,
          case 
                 when  substr(system_ref_no,0,4)='NJRI' then 'NJRI'
                 end as AGG_NAME,
                 transaction_status,date(final_status_change_date) as final_status_change_date,sum(amount) as NJRI_refund_amount
                  from `sm_recon.ts_recharge_njri_rev_transaction_log`
          where 
          (date (final_status_change_date) >= date_trunc(date_sub((@date), interval 1 day), month) and date(final_status_change_date) <=(@date))
          group by system_ref_no,final_status_change_date,transaction_status,order_no
          
 ) as agg_rev_output
ON wallet_output.Transaction_Id=agg_rev_output.REF_AGG_TRANS_ID
LEFT JOIN
(
  select transaction_id as CC_Tran_id,"Present in CC File" as CC_status from `sm_recon.ts_recharge_customer_care_file`  where date(load_date)= @date
  
) as cc_output
ON cc_output.CC_Tran_id=Transaction_Id
)
UNION ALL
select *,(@date) as recon_date from 
(
select Last_Month_Transaction_Id,Refund_Amount,Refund_Date,Client_Id,Client_Wallet_Id,Trans_Date,Trans_Ref_Num,Comments,Aggregator,
case 
when substr(Last_Month_Transaction_Id,0,4)='NJRI' and REF_AGG_AMOUNT is not null then "Refund received from NJRI aggregator"
when substr(Last_Month_Transaction_Id,0,2)='TW' and REF_AGG_AMOUNT is not null then "Refund received from TW aggregator"
--when  substr(trans_id,0,3)='JIO' and 
else AGG_TRANS_STATUS  end as AGG_STATUS,CC_status
from
(
select trans_id as Last_Month_Transaction_Id ,sum(refund_amt) as Refund_Amount,date(refund_date) as Refund_Date,a.client_id as Client_Id, c.client_wallet_id as Client_Wallet_Id,date(trans_date) as Trans_Date,trans_ref_no as Trans_Ref_Num , comments as Comments,
case 
                when  substr(trans_id,0,2)='TW' then 'THINKWALNUT'
                 when  substr(trans_id,0,4)='NJRI' then 'NJRI'
                 when  substr(trans_id,0,3)='JIO' then 'JIO'
                 end as Aggregator  from prod_dwh.client_refund a, prod_dwh.distributor_retailer b , prod_dwh.client_wallet c where
a.client_id= b.retailer_id and b.distributor_id= c.client_id and refund_type="Recharge" and 
(date (refund_date) between  DATE_SUB(DATE_TRUNC(@date, MONTH), INTERVAL 11 DAY)  and DATE_SUB(DATE_TRUNC(@date, MONTH), INTERVAL 1 DAY))
group by trans_id,date(refund_date),client_id,client_wallet_id,date(trans_date),trans_ref_no , comments
) as last_mnth_refund_data
LEFT OUTER JOIN
(
select AGG_TRANS_ID,AGG_TRANS_STATUS,AGG_AMOUNT
from 
 (select client_txn_id as AGG_TRANS_ID,txn_id as AGG_Trans_Ref_No, case 
                when  substr(client_txn_id,0,2)='TW' then 'TW'
                 end as AGG_NAME ,status as AGG_TRANS_STATUS,date(request_timestamp) as AGG_DATE,sum(amount) as AGG_AMOUNT from `sm_recon.ts_recharge_think_wallet_log`where 
(date (request_timestamp) between  DATE_SUB(DATE_TRUNC(@date, MONTH), INTERVAL 11 DAY)          and DATE_SUB(DATE_TRUNC(@date, MONTH), INTERVAL 1 DAY))
          and status in ('Failed','Pending','Rollback')
          group by status,client_txn_id,date(request_timestamp),txn_id
          UNION ALL
          select system_ref_no,order_no,
          case 
                 when  substr(system_ref_no,0,4)='NJRI' then 'NJRI'
                 end as AGG_NAME,
                 recharge_status,order_date,sum(amount)
                  from `sm_recon.ts_recharge_njri_transaction_log`
          where 
         (date (order_date) between  DATE_SUB(DATE_TRUNC(@date, MONTH), INTERVAL 11 DAY)          and DATE_SUB(DATE_TRUNC(@date, MONTH), INTERVAL 1 DAY))and recharge_status in (
'Recharge Unsuccessful','Recharge Inprocess')
          group by system_ref_no,order_date,recharge_status,order_no
          UNION ALL
          select refill_id,trans_id,
          case 
                 when  substr(refill_id,0,3)='JIO' then 'JIO'
                 end as AGG_NAME ,result_description,date(real_time),sum(amount)
           from `sm_recon.ts_recharge_jio_spice_money_log`
          where  (date (real_time) between  DATE_SUB(DATE_TRUNC(@date, MONTH), INTERVAL 11 DAY)          and DATE_SUB(DATE_TRUNC(@date, MONTH), INTERVAL 1 DAY))
           and result_description in ('Transaction Limit Reached for Recharges')
            group by refill_id,result_description,date(real_time),trans_id
 ))as last_month_agg_txn_output
 ON last_mnth_refund_data.Last_Month_Transaction_Id=last_month_agg_txn_output.AGG_TRANS_ID
LEFT OUTER JOIN
(
     select client_txn_id as REF_AGG_TRANS_ID,txn_id as REF_AGG_Trans_Ref_No, 
  case 
                when  substr(client_txn_id,0,2)='TW' then 'TW'
                 end as REF_AGG_NAME ,status as REF_AGG_TRANS_STATUS,date(response_timestamp) as REF_AGG_DATE,sum(amount) as REF_AGG_AMOUNT from `sm_recon.ts_recharge_think_wallet_refund_log`
          where 
        (date (response_timestamp) between  DATE_SUB(DATE_TRUNC(@date, MONTH), INTERVAL 11 DAY)          and DATE_SUB(DATE_TRUNC(@date, MONTH), INTERVAL 1 DAY))
          group by status,client_txn_id,date(response_timestamp),txn_id
          UNION ALL
          select system_ref_no,order_no,
          case 
                 when  substr(system_ref_no,0,4)='NJRI' then 'NJRI'
                 end as AGG_NAME,
                 transaction_status,date(final_status_change_date) as final_status_change_date,sum(amount)
                  from `sm_recon.ts_recharge_njri_rev_transaction_log`
          where 
         (date (final_status_change_date) between  DATE_SUB(DATE_TRUNC(@date, MONTH), INTERVAL 11 DAY)          and DATE_SUB(DATE_TRUNC(@date, MONTH), INTERVAL 1 DAY)) group by system_ref_no,final_status_change_date,transaction_status,order_no
          
 ) as last_month_agg_rev_output
ON last_mnth_refund_data.Last_Month_Transaction_Id=last_month_agg_rev_output.REF_AGG_TRANS_ID
LEFT JOIN
(
  select transaction_id AS cc_tran_id,"Present in CC File" as CC_status from `sm_recon.ts_recharge_customer_care_file` 
  where date(load_date)= @date
  
) as last_month_cc_output
ON last_month_cc_output.cc_tran_id=Last_Month_Transaction_Id
)

    '''
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_spice_vs_tw_njri_cc_refund', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_spice_vs_tw_njri_cc_refund', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    query_job = client.query(sql_query, job_config=job_config2)
    print("Loading of the ts_recharge_spice_vs_tw_njri_cc_refund completed")
    print("Loading of the prod_recharge_spice_vs_tw_njri_cc_refund completed")
    print("---------------------------------------------------------------")
    
    ################################################################################################################
    #Recon Tracker outputopenpyxl 
    ###############################################################################################################
    print("Loading of the ts_recharge_recon_tracker started")
    
    
    sql_query='''
   select B.Transaction_Date as Tran_Date,Txn_Count_As_Per_Wallet_Logs,Txn_Amount_As_Per_Wallet_Logs_NJRI,Txn_Amount_As_Per_Wallet_Logs_TW,Txn_Amount_As_Per_Wallet_Logs_JIO,
(Txn_Amount_As_Per_Wallet_Logs_NJRI+Txn_Amount_As_Per_Wallet_Logs_TW+Txn_Amount_As_Per_Wallet_Logs_JIO) as  Net_Txn_Amount_As_Per_Wallet_Logs, Sum_of_agent_refund_NJRI,Sum_of_agent_refund_TW,Sum_of_agent_refund_JIO, 
(Sum_of_agent_refund_NJRI+Sum_of_agent_refund_TW+Sum_of_agent_refund_JIO) as  Net_Sum_of_agent_refund ,Agent_Commission_as_per_wallet_credit_NJRI,Agent_Commission_as_per_wallet_credit_TW,Agent_Commission_as_per_wallet_credit_JIO,
Agent_Commission_Wallet_LessThan_200_MobileOPR,Agent_Commission_Wallet_AboveOrEqual_200_MobileOPR,Agent_Commission_Wallet_MobileDevice_Airtel_JIO, Agent_Commission_As_Per_Wallet_Credit_DTH_Operator,(Agent_Commission_Wallet_LessThan_200_MobileOPR+Agent_Commission_Wallet_AboveOrEqual_200_MobileOPR+Agent_Commission_Wallet_MobileDevice_Airtel_JIO+Agent_Commission_As_Per_Wallet_Credit_DTH_Operator)as 
Net_Commission_Agent_Wallet_Credit_As_Per_System ,Net_Commission_Agent_Wallet_Reversal_as_per_System,
NJRI_Recharge_Consumption,TW_Recharge_Consumption,JIO_Recharge_Consumption,(NJRI_Recharge_Consumption+TW_Recharge_Consumption+JIO_Recharge_Consumption)as  Net_Aggregator_Consumption ,NJRI_Refund_Amount,TW_Refund_Amount,JIO_Refund_Amount,(NJRI_Refund_Amount+TW_Refund_Amount+JIO_Refund_Amount) as  Net_Aggregator_Refund,
 NJRI_Commission_Credit, NJRI_Commission_Reversal, TW_Commission_Credit, TW_Commission_Reversal,TW_Margin_to_Spice,NJRI_Margin_to_Spice,
(NJRI_Commission_Credit-Agent_Commission_as_per_wallet_credit_NJRI) as Net_NJRI_Commission_Income,
(TW_Commission_Credit-Agent_Commission_as_per_wallet_credit_TW) as  Net_TW_Commission_Income 
from 
(
  select count(*) as  Txn_Count_As_Per_Wallet_Logs ,date(trans_date) as Transaction_Date from prod_dwh.wallet_trans  where (date (trans_date) =@date) and comments in ('Recharge_Mobile')
        group by Transaction_Date 
)as B,
(
  select round(Txn_Amount_As_Per_Wallet_Logs_NJRI,3) as Txn_Amount_As_Per_Wallet_Logs_NJRI,Transaction_Date from   
   ( 
select date(trans_date) as Transaction_Date,sum(trans_amt) as  Txn_Amount_As_Per_Wallet_Logs_NJRI  from prod_dwh.wallet_trans where 
date (trans_date)= @date and comments in ('Recharge_Mobile') and substr(trans_id,0,4)='NJRI'
          group by Transaction_Date
   )

)as C,
(

select round(Txn_Amount_As_Per_Wallet_Logs_TW,3) as Txn_Amount_As_Per_Wallet_Logs_TW,Transaction_Date from   
   ( 
 select date(trans_date) as Transaction_Date,sum(trans_amt) as  Txn_Amount_As_Per_Wallet_Logs_TW  from prod_dwh.wallet_trans where 
date (trans_date)= @date  and comments in ('Recharge_Mobile') and substr(trans_id,0,2)='TW'
          group by Transaction_Date)
)as D,
(
  select round(Txn_Amount_As_Per_Wallet_Logs_JIO,3) as Txn_Amount_As_Per_Wallet_Logs_JIO from   
   ( 
select date(trans_date) as Transaction_Date,sum(trans_amt) as  Txn_Amount_As_Per_Wallet_Logs_JIO  from prod_dwh.wallet_trans where 
date (trans_date)=@date  and comments in ('Recharge_Mobile') and substr(trans_id,0,3)='JIO'
          group by Transaction_Date)
)as E,
(
  select coalesce(
(select round(Sum_of_agent_refund_NJRI,3) as Sum_of_agent_refund_NJRI from   
   ( 
  select sum(refund_amt) as Sum_of_agent_refund_NJRI
                from prod_dwh.client_refund a, prod_dwh.distributor_retailer b , prod_dwh.client_wallet c where
                a.client_id= b.retailer_id and b.distributor_id= c.client_id and refund_type="Recharge" and substr(trans_id,0,4)='NJRI' and date(refund_date)=@date
                group by date(refund_date))),0) as Sum_of_agent_refund_NJRI
)as G,
(
  select coalesce(
(select round(Sum_of_agent_refund_TW,3) as Sum_of_agent_refund_TW from   
   ( 
select sum(refund_amt) as Sum_of_agent_refund_TW
                from prod_dwh.client_refund a, prod_dwh.distributor_retailer b , prod_dwh.client_wallet c where
                a.client_id= b.retailer_id and b.distributor_id= c.client_id and refund_type="Recharge" and substr(trans_id,0,2)='TW' and date(refund_date)=@date
                group by date(refund_date))),0) as Sum_of_agent_refund_TW
)as H,
(
select coalesce(
(select round(Sum_of_agent_refund_JIO,3) as Sum_of_agent_refund_JIO from   
   ( 
select sum(refund_amt) as Sum_of_agent_refund_JIO
                from prod_dwh.client_refund a, prod_dwh.distributor_retailer b , prod_dwh.client_wallet c where
                a.client_id= b.retailer_id and b.distributor_id= c.client_id and refund_type="Recharge" and substr(trans_id,0,3)='JIO' and date(refund_date)=@date
                group by date(refund_date))),0) as Sum_of_agent_refund_JIO
)as I,
(
  select coalesce((
    select round(Agent_Commission_as_per_wallet_credit_NJRI,3) as Agent_Commission_as_per_wallet_credit_NJRI from   (select 
sum(t1.amount_transferred) as Agent_Commission_as_per_wallet_credit_NJRI
FROM spicemoney-dwh.prod_dwh.cme_wallet as t1
        where t1.comments IN ('RECHARGE-Discount-Mobile') and 
         date(transfer_date)=@date and substr(trans_ref_no,0,4)='NJRI' group by date(transfer_date)
         )),0) as Agent_Commission_as_per_wallet_credit_NJRI 
) as K,
(
 select coalesce((
   select round(Agent_Commission_as_per_wallet_credit_TW,3) as Agent_Commission_as_per_wallet_credit_TW from   
   (select 
sum(t1.amount_transferred) as Agent_Commission_as_per_wallet_credit_TW
FROM spicemoney-dwh.prod_dwh.cme_wallet as t1
        where t1.comments IN ('RECHARGE-Discount-Mobile') and 
         date (transfer_date) =@date and substr(trans_ref_no,0,2)='TW' group by date(transfer_date))),0) as Agent_Commission_as_per_wallet_credit_TW
) as L,
(
  select coalesce(( 
   select round(Agent_Commission_as_per_wallet_credit_JIO,3) as Agent_Commission_as_per_wallet_credit_JIO from   
   (  
    select 
sum(t1.amount_transferred) as Agent_Commission_as_per_wallet_credit_JIO 
FROM spicemoney-dwh.prod_dwh.cme_wallet as t1
        where t1.comments IN ('RECHARGE-Discount-Mobile') and 
         date (transfer_date)=@date and substr(trans_ref_no,0,3)='JIO' group by date(transfer_date))),0) as 
         Agent_Commission_as_per_wallet_credit_JIO
) as M,
(

select coalesce(( 
select round(Agent_Commission_Wallet_LessThan_200_MobileOPR,3) as Agent_Commission_Wallet_LessThan_200_MobileOPR from   
   (  
select sum(ftr_commission) as Agent_Commission_Wallet_LessThan_200_MobileOPR from 
(select FTR_UNIQUE_IDENTIFICATION_NO,ftr_commission,device_type,OPERATOR_TITLE,recharge_amt from 
(select amount_transferred as ftr_commission,
    unique_identification_no as FTR_UNIQUE_IDENTIFICATION_NO from 
    spicemoney-dwh.prod_dwh.cme_wallet
            where comments IN ('RECHARGE-Discount-Mobile') and 
            DATE(transfer_date) = @date
)as FTR_revoke_output
LEFT OUTER JOIN
    (
    select t1.trans_id,t1.device_type,t1.operator_id,t1.recharge_amt,t2.name as OPERATOR_TITLE from prod_dwh.recharge t1, `prod_dwh.operator` t2
    where t1.operator_id=t2.operator_id
    ) as recharge_output
    ON recharge_output.trans_id=FTR_UNIQUE_IDENTIFICATION_NO
)
where recharge_amt<200
and OPERATOR_TITLE NOT IN ('AIRTEL','RELIANCE_JIO')


)),0) as  Agent_Commission_Wallet_LessThan_200_MobileOPR
) as N,
(
  
select coalesce(( 
  select round(Agent_Commission_Wallet_AboveOrEqual_200_MobileOPR,3) as Agent_Commission_Wallet_AboveOrEqual_200_MobileOPR from   
   ( 
select sum(ftr_commission) as Agent_Commission_Wallet_AboveOrEqual_200_MobileOPR from 
(select FTR_UNIQUE_IDENTIFICATION_NO,ftr_commission,device_type,OPERATOR_TITLE,recharge_amt from 
(select amount_transferred as ftr_commission,
    unique_identification_no as FTR_UNIQUE_IDENTIFICATION_NO from 
    spicemoney-dwh.prod_dwh.cme_wallet
            where comments IN ('RECHARGE-Discount-Mobile') and 
            DATE(transfer_date) = @date
)as FTR_revoke_output
LEFT OUTER JOIN
    (
    select t1.trans_id,t1.device_type,t1.operator_id,t1.recharge_amt,t2.name as OPERATOR_TITLE from prod_dwh.recharge t1, `prod_dwh.operator` t2
    where t1.operator_id=t2.operator_id
    ) as recharge_output
    ON recharge_output.trans_id=FTR_UNIQUE_IDENTIFICATION_NO
)
where recharge_amt>199 and OPERATOR_TITLE NOT IN ('AIRTEL','RELIANCE_JIO')
)),0) as  Agent_Commission_Wallet_AboveOrEqual_200_MobileOPR


) as O,
(
select coalesce(( 
  select round(Agent_Commission_Wallet_MobileDevice_Airtel_JIO,3) as Agent_Commission_Wallet_MobileDevice_Airtel_JIO from   
   ( 
select sum(ftr_commission) as Agent_Commission_Wallet_MobileDevice_Airtel_JIO from 
(select FTR_UNIQUE_IDENTIFICATION_NO,ftr_commission,device_type,OPERATOR_TITLE,recharge_amt from 
(select amount_transferred as ftr_commission,
    unique_identification_no as FTR_UNIQUE_IDENTIFICATION_NO from 
    spicemoney-dwh.prod_dwh.cme_wallet
            where comments IN ('RECHARGE-Discount-Mobile') and 
            DATE(transfer_date) = @date
)as FTR_revoke_output
LEFT OUTER JOIN
    (
    select t1.trans_id,t1.device_type,t1.operator_id,t1.recharge_amt,t2.name as OPERATOR_TITLE from prod_dwh.recharge t1, `prod_dwh.operator` t2
    where t1.operator_id=t2.operator_id
    ) as recharge_output
    ON recharge_output.trans_id=FTR_UNIQUE_IDENTIFICATION_NO
)
where OPERATOR_TITLE IN ('AIRTEL','RELIANCE_JIO')

)),0) as  Agent_Commission_Wallet_MobileDevice_Airtel_JIO

)as Q,
(

 select coalesce(null,0) as Agent_Commission_As_Per_Wallet_Credit_DTH_Operator
) as P,
(
select coalesce((
  select round(Net_Commission_Agent_Wallet_Reversal_as_per_System,3) as Net_Commission_Agent_Wallet_Reversal_as_per_System from   
   ( 
  select 
sum(t1.amount_transferred) as Net_Commission_Agent_Wallet_Reversal_as_per_System
FROM spicemoney-dwh.prod_dwh.cme_wallet as t1
        where t1.comments IN ('RECHARGE-Discount-Mobile-Reversal') and 
         date(transfer_date)=@date group by date(transfer_date))),0) as Net_Commission_Agent_Wallet_Reversal_as_per_System 

)as S,
(

select coalesce((
  select round(NJRI_Recharge_Consumption,3) as NJRI_Recharge_Consumption from   
   ( 
  select sum(amount) as NJRI_Recharge_Consumption from `sm_recon.ts_recharge_njri_transaction_log`
          where date (order_date) =@date
          and recharge_status in ('Recharge Successful'))),0) as  NJRI_Recharge_Consumption 
)as T,
(
select coalesce((
  select round(TW_Recharge_Consumption,3) as TW_Recharge_Consumption from   
   ( 
  select sum(amount) as TW_Recharge_Consumption from `sm_recon.ts_recharge_think_wallet_log`
          where date (request_timestamp) =@date
          and status in ('Success'))),0) as  TW_Recharge_Consumption 
)as U,
(
select coalesce((
  select round(JIO_Recharge_Consumption,3) as JIO_Recharge_Consumption from   
   ( 
  select sum(amount) as JIO_Recharge_Consumption from `sm_recon.ts_recharge_jio_spice_money_log`
          where date (real_time) =@date
          and result_description in ('Transaction Successful'))),0) as  JIO_Recharge_Consumption
)as V,
(
  select coalesce((
    select round(NJRI_Refund_Amount,3) as NJRI_Refund_Amount from   
   ( select sum(amount) as  NJRI_Refund_Amount from `sm_recon.ts_recharge_njri_rev_transaction_log` where date (final_status_change_date) =@date)),0) as NJRI_Refund_Amount

)as X,
(
select coalesce((
  select round(TW_Refund_Amount,3) as TW_Refund_Amount from   
   ( select sum(amount) as  TW_Refund_Amount  from `sm_recon.ts_recharge_think_wallet_refund_log` where date (response_timestamp) =@date)),0) as TW_Refund_Amount

) as Y,
(
select coalesce((
  select round(JIO_Refund_Amount,3) as JIO_Refund_Amount from   
   ( 
  select sum(amount) as JIO_Refund_Amount from `sm_recon.ts_recharge_jio_spice_money_log`
where  date (real_time) =@date and result_description in ('Transaction Limit Reached for Recharges')) ),0) as JIO_Refund_Amount

) as Z,
(

select coalesce((
  select round(NJRI_Commission_Credit,3) as NJRI_Commission_Credit from   
   ( select sum(commission_amount) as NJRI_Commission_Credit from `sm_recon.ts_recharge_njri_commission_calculate_report`
where  date(order_date) =@date)),0) as NJRI_Commission_Credit

) as AB, 
(
  
select coalesce((
  select round(NJRI_Commission_Reversal,3) as NJRI_Commission_Reversal from   
   ( select sum(commission_amount) as NJRI_Commission_Reversal from `sm_recon.ts_recharge_njri_reversed_commission_report`
where  date(reversal_date) =@date)),0) as NJRI_Commission_Reversal
  
) as AC, 
(
  
  select coalesce((
    select round(TW_Commission_Credit,3) as TW_Commission_Credit from   
   ( 
  select sum(TW_Commission_Amount) as  TW_Commission_Credit from
  (
 select amount,amount_deducted,amount-amount_deducted as TW_Commission_Amount from `sm_recon.ts_recharge_think_wallet_log` where status in ('Success','Rollback','Pending') and date(request_timestamp)=@date
  )


  )),0) as TW_Commission_Credit
  
) as AD,
(
  
  select coalesce((
    select round(TW_Commission_Reversal_Amount,3) as TW_Commission_Reversal_Amount from   
   ( 
  select sum(TW_Commission_Reversal_Amount ) as  TW_Commission_Reversal_Amount  from
  (
 select amount-amount_deducted as TW_Commission_Reversal_Amount,client_txn_id
  from `sm_recon.ts_recharge_think_wallet_log` where status in ('Pending','Rollback') and date(response_timestamp)=@date
  )
  )),0) as TW_Commission_Reversal
  
) as AE,
(
  select coalesce((
   select round(TW_Margin_to_Spice,3) as TW_Margin_to_Spice from   
   ( 
  select sum(margin_amt) as  TW_Margin_to_Spice
 from `sm_recon.ts_recharge_think_wallet_recharge_sales_summary` where date(filedate)=@date)
  ),0) as TW_Margin_to_Spice
) as AF,
(
  select coalesce((
   select round(NJRI_Margin_to_Spice,3) as NJRI_Margin_to_Spice from   
   ( 
  select margin as NJRI_Margin_to_Spice 
 from `sm_recon.ts_recharge_njri_recharge_sales_summary` where date(load_date)=@date and operator="Total"
   )),0) as NJRI_Margin_to_Spice
) as AG



    
    '''
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.ts_recharge_recon_tracker', write_disposition='WRITE_APPEND' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_recharge_recon_tracker', write_disposition='WRITE_APPEND' ,  query_parameters=[
    bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    query_job = client.query(sql_query, job_config=job_config2)
    print("Loading of the ts_recharge_recon_tracker completed")
    print("Loading of the prod_recharge_recon_tracker completed")
    
    print("---------------------------------------------------------------")
    

    
main()
    


# In[ ]:




