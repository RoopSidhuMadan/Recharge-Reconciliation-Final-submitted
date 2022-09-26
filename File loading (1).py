#!/usr/bin/env python
# coding: utf-8

# In[18]:


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

    date = d.today()
    current_day = date.strftime('%d')
    
    date = d.today()-timedelta(1)
    previous_day = date.strftime('%d')

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
   
    #---------------------------------------------------------------------------------------------------------------------
    #Loading NJRIRechargeCommissionReport summary
    #---------------------------------------------------------------------------------------------------------------------
    '''
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
  
    
    load_month='08'   
    print('gs://sm-prod-rpa/'+str(current_year)+'/'+str(load_month)+'/'+'11'+'/NJRIRechargeCommissionReport/CommissionReport_'+'*.xlsx')
            # Reading data from excel to dataframe            
    df_njrisummary = pd.read_excel('gs://sm-prod-rpa/'+str(current_year)+'/'+str(load_month)+'/'+'11'+'/NJRIRechargeCommissionReport/CommissionReport_'+'*.xlsx',skiprows=1,names=header_list_njrisummary,storage_options={"token": key_path},header=None)  
    
    
    df_njrisummary[list1_njrisummary]=df_njrisummary[list1_njrisummary].astype(str)
    df_njrisummary[list2_njrisummary]=df_njrisummary[list2_njrisummary].astype(float)
    df_njrisummary['load_date']="2022-08-10"
   

    df_njrisummary.to_gbq(destination_table='sm_recon.ts_recharge_njri_recharge_sales_summary', project_id='spicemoney-dwh', if_exists='replace' , table_schema = schema_njrisummary,credentials=credentials)
    print("Data moved to ts_recharge_njri_recharge_sales_summary table")
    df_njrisummary.to_gbq(destination_table='prod_sm_recon.prod_recharge_njri_recharge_sales_summary', project_id='spicemoney-dwh', if_exists='replace' , table_schema = schema_njrisummary,credentials=credentials)
    print("Data moved to prod_recharge_njri_recharge_sales_summary table")

    

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
   
    load_month='08'   
    print('gs://sm-prod-rpa/'+str(current_year)+'/'+str(load_month)+'/'+'11'+'/Think WalletRechargesales_summary/sales_summary.csv')
            # Reading data from excel to dataframe            
    df_twsummary = pd.read_csv('gs://sm-prod-rpa/'+str(current_year)+'/'+str(load_month)+'/'+'11'+'/Think WalletRechargesales_summary/sales_summary.csv',skiprows=2,names=header_list_twsummary,storage_options={"token": key_path},header=None,parse_dates = (['filedate']), low_memory=False)  
    print(df_twsummary.info())
    #df_twsummary['filedate'] = pd.to_datetime(df_twsummary['filedate'])
    df_twsummary[list1_twsummary]=df_twsummary[list1_twsummary].astype(str)
    df_twsummary[list2_twsummary]=df_twsummary[list2_twsummary].astype(float)
    print(df_twsummary.info())
   

    df_twsummary.to_gbq(destination_table='sm_recon.ts_recharge_think_wallet_recharge_sales_summary', project_id='spicemoney-dwh', if_exists='replace' , table_schema = schema_twsummary,credentials=credentials)
    print("Data moved to ts_recharge_think_wallet_recharge_sales_summary table")
    df_twsummary.to_gbq(destination_table='prod_sm_recon.prod_recharge_think_wallet_recharge_sales_summary', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_twsummary,credentials=credentials)
    print("Data moved to prod_recharge_think_wallet_recharge_sales_summary table")

    '''
    
    '''
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
    
    load_day = int(16)
    for day in range(load_day, load_day-2, -1):
        
            print('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(day)+'/Think WalletRechargeLogs/logs.csv')
            # Reading data from excel to dataframe            
            df = pd.read_csv('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(day)+'/Think WalletRechargeLogs/logs.csv',skiprows=1,names=header_list,storage_options={"token": key_path},header=None,parse_dates = (['request_timestamp','response_timestamp']), low_memory=False)  


            df['client_txn_id']=df['client_txn_id'].str.replace(r"'",'')

            df[list1]=df[list1].astype(str)
            df[list2]=df[list2].astype(float)

            df.to_gbq(destination_table='sm_recon.ts_recharge_think_wallet_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema,credentials=credentials)
            print("Data moved to ts_recharge_think_wallet_log table")
            df.to_gbq(destination_table='prod_sm_recon.prod_recharge_think_wallet_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema,credentials=credentials)
            print("Data moved to prod_recharge_think_wallet_log table")
    
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
    
    load_day = int(16)
    
    for day in range(load_day, load_day-2, -1):

        print('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(day)+'/NJRIRechargeTransactionReport/TransactionReport.xlsx')         
        df_njri = pd.read_excel('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(day)+'/NJRIRechargeTransactionReport/TransactionReport.xlsx',skiprows=1,names=header_list_njri,storage_options={"token": key_path},header=None)             

        df_njri['order_date'] = pd.to_datetime(df_njri['order_date'])

        df_njri['order_date']=pd.to_datetime(df_njri['order_date'].dt.strftime('%d-%m-%Y'))


        df_njri['system_ref_no']=df_njri['system_ref_no'].str.replace(r"'",'')
        df_njri[list1_njri]=df_njri[list1_njri].astype(str)
        df_njri[list2_njri]=df_njri[list2_njri].astype(float)

        df_njri.to_gbq(destination_table='sm_recon.ts_recharge_njri_transaction_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_njri,credentials=credentials)
        print("Data moved to ts_recharge_njri_transaction_log table")
        df_njri.to_gbq(destination_table='prod_sm_recon.prod_recharge_njri_transaction_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_njri,credentials=credentials)
        print("Data moved to prod_recharge_njri_transaction_log table")
    
   '''
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
   
    load_day = int(14)
    
    for day in range(load_day, load_day-1, -1):
        print('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(day)+'/EmailReports/JIO_JIO/Spice Money_'+str(day-1)+' '+ str(current_mon)+' '+str(current_yr) +'.xlsxb')

        df_jio=pd.read_excel('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(day)+'/EmailReports/JIO_JIO/Spice Money_'+str(day-1)+' '+ str(current_mon)+' '+str(current_yr) +'.xlsxb', dtype=object,storage_options={"token": key_path}, skiprows=1,names=header_list_jio,header=None)

        df_jio['real_time'] = pd.TimedeltaIndex(df_jio['transaction_time'], unit='d') + dt.datetime(1899, 12, 30)
        df_jio['real_time']=pd.to_datetime(df_jio['real_time'].dt.strftime('%Y-%m-%d %H:%M:%S'))
        df_jio[list1_jio]=df_jio[list1_jio].astype(str)
        df_jio[list2_jio]=df_jio[list2_jio].astype(float)
        df_jio['refill_id']=df_jio['refill_id'].str.replace(r"'",'')


        df_jio.to_gbq(destination_table='sm_recon.ts_recharge_jio_spice_money_log', project_id='spicemoney-dwh', if_exists='append', table_schema = schema_jio,credentials=credentials)
        print("Data moved to ts_recharge_jio_spice_money_log table")
        df_jio.to_gbq(destination_table='prod_sm_recon.prod_recharge_jio_spice_money_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_jio,credentials=credentials)
        print("Data moved to prod_recharge_jio_spice_money_log table")
        
      
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
   
    load_day = int(14)
    
    for day in range(load_day, load_day-1, -1):
        print('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(day)+'/EmailReports/NJRI_Recharge/TransactionReversalReport-283'+"-"+str(day) +"-"+str(current_month)+"-"+str(current_year) +"*.xlsx")
                                                                                    
        df_njri_rev = pd.read_excel('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(day)+'/EmailReports/NJRI_Recharge/TransactionReversalReport-283'+"-"+str(day) +"-"+str(current_month)+"-"+str(current_year) +"*.xlsx",skiprows=1,names=header_list_njri_rev ,storage_options={"token": key_path},header=None,parse_dates = (['order_date','final_status_change_date']))             


        df_njri_rev [list1_njri_rev]=df_njri_rev[list1_njri_rev].astype(str)
        df_njri_rev [list2_njri_rev]=df_njri_rev[list2_njri_rev].astype(float)
        df_njri_rev['system_ref_no']=df_njri_rev['system_ref_no'].str.replace(r"'",'')


        df_njri_rev.to_gbq(destination_table='sm_recon.ts_recharge_njri_rev_transaction_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_njri_rev ,credentials=credentials)
        print("Data moved to ts_recharge_njri_rev_transaction_log table")
        df_njri_rev.to_gbq(destination_table='prod_sm_recon.prod_recharge_njri_rev_transaction_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_njri_rev,credentials=credentials)
        print("Data moved to prod_recharge_njri_rev_transaction_log table")
    
    '''
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
  
    load_day = int(16)
    
    for day in range(load_day, load_day-2, -1):
        print('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(day)+'/Think WalletRechargeRefund file/refund.csv')
        df_tw_ref = pd.read_csv('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(day)+'/Think WalletRechargeRefund file/refund.csv',skiprows=1,names=header_list_tw_ref ,storage_options={"token": key_path},header=None,
                                    parse_dates = (['request_timestamp','response_timestamp']), low_memory=False)             


        df_tw_ref[list1_tw_ref]=df_tw_ref[list1_tw_ref].astype(str)
        df_tw_ref[list2_tw_ref]=df_tw_ref[list2_tw_ref].astype(float)
        df_tw_ref['client_txn_id']=df_tw_ref['client_txn_id'].str.replace(r"'",'')


        df_tw_ref.to_gbq(destination_table='sm_recon.ts_recharge_think_wallet_refund_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_tw_ref,credentials=credentials)
        print("Data moved to ts_recharge_think_wallet_refund_log table")

        df_tw_ref.to_gbq(destination_table='prod_sm_recon.prod_recharge_think_wallet_refund_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_tw_ref,credentials=credentials)
        print("Data moved to prod_recharge_think_wallet_refund_log table")



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
    load_day = int(11)
    load_month='08'
    for day in range(load_day, load_day-1, -1):
        print('gs://sm-prod-rpa/'+str(current_year)+'/'+str(load_month)+'/'+str(day)+'/EmailReports/NJRI_Recharge/ReversedCommissionReport_*.xlsx')
        df_njri_revcom = pd.read_excel('gs://sm-prod-rpa/'+str(current_year)+'/'+str(load_month)+'/'+str(day)+'/EmailReports/NJRI_Recharge/ReversedCommissionReport_*.xlsx',skiprows=1,names=header_list_njri_revcom ,storage_options={"token": key_path},
                                         header=None,sheet_name='Report', parse_dates = (['order_date','reversal_date']))             

        df_njri_revcom[list1_njri_revcom]=df_njri_revcom[list1_njri_revcom].astype(str)
        df_njri_revcom[list2_njri_revcom]=df_njri_revcom[list2_njri_revcom].astype(float)
        df_njri_revcom['system_reference_no']=df_njri_revcom['system_reference_no'].str.replace(r"'",'')
        df_njri_revcom.to_gbq(destination_table='sm_recon.ts_recharge_njri_reversed_commission_report', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_njri_revcom,credentials=credentials)
        print("Data moved to ts_recharge_njri_reversed_commission_report table")

        df_njri_revcom.to_gbq(destination_table='prod_sm_recon.prod_recharge_njri_reversed_commission_report', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_njri_revcom,credentials=credentials)
        print("Data moved to prod_recharge_njri_reversed_commission_report table")

    
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
    load_day = int(11)
    load_month='08'
    for day in range(load_day, load_day-1, -1):
        print('gs://sm-prod-rpa/'+str(current_year)+'/'+str(load_month)+'/'+str(day)+'/EmailReports/NJRI_Recharge/CommissionCalculateReport_*.xlsx')
        df_njri_com = pd.read_excel('gs://sm-prod-rpa/'+str(current_year)+'/'+str(load_month)+'/'+str(day)+'/EmailReports/NJRI_Recharge/CommissionCalculateReport_*.xlsx',sheet_name='Report',skiprows=1,names=header_list_njri_com ,storage_options={"token": key_path},
                                     header=None , parse_dates = (['order_date']))             


        df_njri_com[list1_njri_com]=df_njri_com[list1_njri_com].astype(str)
        df_njri_com[list2_njri_com]=df_njri_com[list2_njri_com].astype(float)
        df_njri_com['system_reference_no']=df_njri_com['system_reference_no'].str.replace(r"'",'')

        df_njri_com.to_gbq(destination_table='sm_recon.ts_recharge_njri_commission_calculate_report', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_njri_com,credentials=credentials)
        print("Data moved to ts_recharge_njri_commission_calculate_report table")

        df_njri_com.to_gbq(destination_table='prod_sm_recon.prod_recharge_njri_commission_calculate_report', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema_njri_com,credentials=credentials)
        print("Data moved to prod_recharge_njri_commission_calculate_report table")
        '''  
main()
 


# 
