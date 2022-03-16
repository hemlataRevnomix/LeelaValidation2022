import pandas as pd
from datetime import datetime,timedelta
import warnings
warnings.filterwarnings("ignore")

def fileCheck(filename,fltype,file,xDate, cl_htl):
    if filename != None:
        if fltype == 'Financial':
            try:
                df_pipefile = pd.read_csv(filename, sep="|" , index_col=False,  error_bad_lines=False,  encoding="utf-8")
            except:
                df_pipefile = pd.read_csv(filename, sep="|" , index_col=False,   error_bad_lines=False,encoding="utf-8")

            file.write("\nFile Name:{}".format(filename))
            file.write("\nFile Type:{}".format(fltype))

            colnum = df_pipefile.shape[1]
            maxrows = df_pipefile.shape[0]
            if colnum == 1:
                file.write('\n=============================================================================\n')
                file.write("\n### Plz Check {} file is NOT in pipe Separated ###\n".format(filename))

                file.write('\n==============================================================================\n')
                print("\n### Plz Check {} file is NOT in pipe Separated ###\n".format(filename))
                return ('NA')
            else:
                file.write("\n {} : Pipe Separated and has {} columns ".format(fltype,colnum))

            std_col = ['ORIGINAL_RESV_NAME_ID', 'TRX_DATE', 'F', 'FF', 'O', 'OO', 'L', 'LL']

            fileCols = (list(df_pipefile.columns))

            if set(std_col).issubset(fileCols) == True:
                file.write("\nAll Columns are present in file\n")
            else:
                a = set(std_col).difference(set(fileCols))
                file.write("\nAll Columns are  Not in financial file {}\t filename:{}\n".format(a,filename))
                file.write("\nNew Columns are  found in financial file {}\n".format(a))

            # file.write(str(filename))
            file.write('\n max_row count of dataframe:{}'.format(str(maxrows)))

            for col in df_pipefile.columns:
                if df_pipefile[col].dtype == 'object':
                    try:
                        df_pipefile[col] = pd.to_datetime(df_pipefile[col])
                    except ValueError:
                        pass
            try:
                df_pipefile['TRX_DATE'] = pd.to_datetime(df_pipefile['TRX_DATE'], format='%d-%b-%y')

                date1 = df_pipefile['TRX_DATE'].min().strftime('%d-%b-%y')
                date2 = df_pipefile['TRX_DATE'].max().strftime('%d-%b-%y')

                xDate = datetime.today()
                # valdate1 = xDate - timedelta(days=15)
                # valdate2 = xDate - timedelta(days=1)
                # mindate = df_pipefile['TRX_DATE'].min()
                # maxdate = df_pipefile['TRX_DATE'].max()
                # if mindate == valdate1:
                #     file.write("\n   min date:{} | Status:PASS | Expected: min date = {} ".format(mindate,valdate1))
                #
                # else:
                #     file.write("\n   min date:{} | Status:Failed* | Expected: min date  {}".format(mindate,valdate1))
                #
                # if maxdate == valdate2:
                #     file.write("\n   max date:{} | Status:PASS | Expected: max date = {} ".format(maxdate,valdate2))
                #
                # else:
                #     file.write("\n   max date:{} | Status:Failed* | Expected: max date  {}".format(maxdate,valdate2))



                file.write("\n Date Range:From {} to {} ".format(date1, date2))
                file.write("\n---------------------------------------------------------------------------")
            except Exception as e:
                pass
                # print(fltype,":check date format in file", e)

            file.write('\nfinancial RowCount and DataTypes:\n')
            df_count = df_pipefile.count().reset_index().rename(columns={'index': 'Fields', 0: ' Row_Count'})
            df_dtype = df_pipefile.dtypes.reset_index().rename(columns={'index': 'Fields', 0: ' data_type'})
            df_null_count = df_pipefile.isna().sum().reset_index().rename(columns={'index': 'Fields', 0: 'null_count'})
            df_pipefile_schema = df_count.merge(df_dtype.merge(df_null_count, on='Fields', how='left'))
            file.write(df_pipefile_schema.to_string())
            file.write('\n-------------------------------------------------------------------------------------')

            try:
                f_sum=df_pipefile['F'].sum()
                ff_sum = df_pipefile['FF'].sum()
                o_sum = df_pipefile['O'].sum()
                oo_sum = df_pipefile['OO'].sum()
                l_sum = df_pipefile['L'].sum()
                ll_sum = df_pipefile['LL'].sum()

                file.write("\n Sum of F column:{}".format(float(f_sum)))

                file.write("\n Sum of FF column:{}".format(ff_sum))

                file.write("\n Sum of o column:{}".format(o_sum))

                file.write("\n Sum of oo column:{}".format(oo_sum))

                file.write("\n Sum of l column:{}".format(l_sum))

                file.write("\n Sum of ll column:{}".format(ll_sum))


                # file.write("\n financial numeric colums sum")
                #
                #
                # file.write("\n Sum of O column:{}".format(o_sum))
                # file.write("\n Sum of OO column:{}".format(oo_sum))
                # file.write("\n Sum of L column:{}".format(l_sum))
                # file.write("\n Sum of LL column:{}".format(ll_sum))



            except Exception as e:
                print("Error key mesg:",e)


            # a=df_pipefile.isna().sum()
            # print("Null values:",a)

            # file.write("{}".format(a))


            # mapfile = pd.read_excel(r"C:\Chakradhar\hemlata\Validation project\leela\mapfiles\Leela_LOF_For_Finance.xlsx")
            # my_type = dict(zip(mapfile.Rep_Fields, mapfile.data_type))
            #
            # dtypes1 = df_pipefile.dtypes.to_dict()
            #
            # for key in my_type.items():
            #     if key in dtypes1.items():
            #         print("All data types work correctly")
            #     else:
            #         print("This datatypes are not work properly",my_type[key])

            a=df_pipefile.select_dtypes(include='number').columns.tolist()
            print(a)
            file.write("\n==============================================================================")
        elif fltype == 'history_reservation': ## Changed by AR 29 Jan2021
            try:
                df_Reh = pd.read_csv(filename, sep="|", index_col=False,  error_bad_lines=False,  encoding="utf-8")
            except:
                df_Reh = pd.read_csv(filename, sep="|", index_col=False,  error_bad_lines=False,  encoding="utf-8")

            colnum = df_Reh.shape[1]
            maxrows = df_Reh.shape[0]

            std_col = [
                'BUSINESS_DATE_CREATED','RESERVATION_DATE','ROOM_COST','ARRIVAL','ARRIVAL_TIME','DEPARTURE','DEPARTURE_TIME','CONFIRMATION_NO','RESV_NAME_ID',
                'RESV_STATUS','CANCELLATION_DATE','ROOM_CLASS','ROOM_CATEGORY','BOOKED_ROOM_CATEGORY','NIGHTS','NO_OF_ROOMS','ADULTS','CURRENCY_CODE','RATE_CODE',
                'SHARE_ID','SHARED_YN','MARKET_CODE','COMPANY_NAME','COMPANY_ID','TRAVEL_AGENT_NAME','TRAVEL_AGENT_ID','GUEST_COUNTRY','NATIONALITY','CHANNEL','ORIGIN_OF_BOOKING',
                'SOURCE_ID','SOURCE_NAME']

            fileCols = (list( df_Reh.columns))

            file.write("\nFile Name:{}".format(filename))
            file.write("\nFile Type:{}".format(fltype))


            if colnum == 1:
                file.write('\n=============================================================================\n')
                file.write("\n### Plz Check {} file is NOT in pipe Separated ###\n".format(filename))
                file.write('\n=============================================================================\n')
                print("\n### Plz Check {} file is NOT in pipe Separated ###\n".format(filename))
                return ('NA')

            else:
                file.write("\n {} : Pipe Separated and has {} columns ".format(fltype, colnum))

            # file.write(str(filename))

            if set(std_col).issubset(fileCols) == True:
                file.write("\nAll Columns are present in Reservation_History file\n")
            else:
                a = set(std_col).difference(set(fileCols))
                file.write("\nAll Columns are NOT in Reservation_History file{} \t filename:{}\n".format(a,filename))
                file.write("\nNew Columns Found in Reservation_History file{}\n".format(a))

            file.write('\n max_row count of dataframe:{}'.format(str(maxrows)))

            try:
                 df_Reh['DEPARTURE'] = pd.to_datetime( df_Reh['DEPARTURE'], format='%d-%b-%y')
                 df_Reh['ARRIVAL'] = pd.to_datetime( df_Reh['ARRIVAL'], format='%d-%b-%y')
                 df_Reh['RESERVATION_DATE'] = pd.to_datetime( df_Reh['RESERVATION_DATE'], format='%d-%b-%y')
                 df_Reh['BUSINESS_DATE_CREATED'] = pd.to_datetime(df_Reh['BUSINESS_DATE_CREATED'],
                                                                      format='%d-%b-%y')

                 date1 = df_Reh['RESERVATION_DATE'].min().strftime("%d-%b-%y")
                 date2 = df_Reh['RESERVATION_DATE'].max().strftime("%d-%b-%y")

                 # xDate = datetime.today()
                 # valdate1 = xDate - timedelta(days=15)
                 # valdate2 = xDate - timedelta(days=1)
                 # mindate = df_Reh['RESERVATION_DATE'].min()
                 # maxdate = df_Reh['RESERVATION_DATE'].max()
                 # if mindate == valdate1:
                 #     file.write("\n   min date:{} | Status:PASS | Expected: min date = {} ".format(mindate, valdate1))
                 #
                 # else:
                 #     file.write("\n   min date:{} | Status:Failed* | Expected: min date  {}".format(mindate, valdate1))
                 #
                 # if maxdate == valdate2:
                 #     file.write("\n   max date:{} | Status:PASS | Expected: max date = {} ".format(maxdate, valdate2))
                 #
                 # else:
                 #     file.write("\n   max date:{} | Status:Failed* | Expected: max date  {}".format(maxdate, valdate2))

                 file.write("\n Date Range:From {} to {} ".format(date1, date2))
                 file.write("\n--------------------------------------------------------------------------------")

            except Exception as e:
                pass
               # file.write("check date format in file ".format(e))


            file.write('\nReservation history RowCount and DataTypes:\n')
            df_count = df_Reh.count().reset_index().rename(columns={'index': 'Fields', 0: ' Row_Count'})
            df_dtype = df_Reh.dtypes.reset_index().rename(columns={'index': 'Fields', 0: ' data_type'})
            df_null_count = df_Reh.isna().sum().reset_index().rename(columns={'index': 'Fields', 0: 'null_count'})
            df_pipefile_schema = df_count.merge(df_dtype.merge(df_null_count, on='Fields', how='left'))
            file.write(df_pipefile_schema.to_string())
            file.write("\n----------------------------------------------------------------------------------------------")

            ms_code = ['AIC', 'AIL', 'AIT', 'CMP', 'HSU', 'RFP', 'NRF', 'NRD', 'LST', 'GCR', 'SER', 'INC', 'DLG', 'GLW',
                       'TRR','TRD', 'TRN', 'PKG', 'PKD', 'TAC', 'TCD', 'TAF', 'TLD', 'GLR', 'GLD', 'CHD', 'ZZZ', 'FIT']

            ms_code_file = list(df_Reh['MARKET_CODE'])

            if set(ms_code_file).issubset(ms_code) == True:
                file.write("\n All ms code are present")
            else:
                a = set(ms_code_file).difference(set(ms_code))
                file.write("\nNew mscode are  found in reservation future file {}\n".format(a))



            file.write("\n==============================================================================")

        elif fltype == 'Reservation_Future':
            try:
                df_Resf = pd.read_csv(filename, sep="|", index_col=False,   error_bad_lines=False,encoding="utf-8")
            except:
                df_Resf = pd.read_csv(filename, sep="|", index_col=False,   error_bad_lines=False,encoding="utf-8")

            file.write('\nFile name :{}'.format(str(filename)))
            file.write('\nFile type :{}'.format(fltype))
            colnum = df_Resf.shape[1]
            maxrows = df_Resf.shape[0]

            if colnum == 1:
                file.write('\n=============================================================================\n')
                file.write("\n### Plz Check {} file is NOT in pipe Separated ###\n".format(filename))
                file.write('\n=============================================================================\n')
                print("\n### Plz Check {} file is NOT in pipe Separated ###\n".format(filename))
                return ('NA')
            else:
                file.write(" \n{} : Pipe Separated and has {} columns ".format(fltype,colnum))

            file.write('\n max_row count of dataframe:{}'.format(str(maxrows)))

            std_col = [
                'ROOM_COST', 'NET_ROOM_REVENUE', 'BUSINESS_DATE_CREATED', 'RESERVATION_DATE', 'ARRIVAL', 'ARRIVAL_TIME',
                'DEPARTURE', 'DEPARTURE_TIME', 'CONFIRMATION_NO',
                'RESV_NAME_ID', 'RESV_STATUS', 'CANCELLATION_DATE', 'ROOM_CLASS', 'ROOM_CATEGORY', 'BOOKED_ROOM_CATEGORY',
                'NIGHTS', 'NO_OF_ROOMS', 'ADULTS', 'CURRENCY_CODE',
                'RATE_CODE', 'SHARE_ID', 'SHARED_YN', 'MARKET_CODE', 'COMPANY_NAME', 'COMPANY_ID', 'TRAVEL_AGENT_NAME',
                'GUEST_COUNTRY', 'NATIONALITY', 'CHANNEL', 'ORIGIN_OF_BOOKING',
                'SOURCE_ID', 'SOURCE_NAME', 'ROOM']
            fileCols = (list(df_Resf.columns))


            if set(std_col).issubset(fileCols) == True:
                file.write("\nAll Columns are present in Reservation_Future file\n")
            else:
                a = set(std_col).difference(set(fileCols))
                file.write("\nAll Columns are  NOT in Reservation_Future file {} \t filename:{}\n".format(a,filename))
                file.write("\nNew Columns are  Found in Reservation_Future file {}\n".format(a))


            try:
                df_Resf['DEPARTURE'] = pd.to_datetime(df_Resf['DEPARTURE'], format='%d-%b-%y')
                df_Resf['ARRIVAL'] = pd.to_datetime(df_Resf['ARRIVAL'], format='%d-%b-%y')
                df_Resf['RESERVATION_DATE'] = pd.to_datetime(df_Resf['RESERVATION_DATE'], format='%d-%b-%y')
                df_Resf['BUSINESS_DATE_CREATED'] = pd.to_datetime(df_Resf['BUSINESS_DATE_CREATED'],
                                                                      format='%d-%b-%y')

                date1 = df_Resf['RESERVATION_DATE'].min().strftime("%d-%b-%y")
                date2 = df_Resf['RESERVATION_DATE'].max().strftime("%d-%b-%y")

                xDate = datetime.today()
                # valdate1 = xDate
                # valdate2 = xDate + timedelta(days=365)
                # mindate = df_Resf['RESERVATION_DATE'].min()
                # maxdate = df_Resf['RESERVATION_DATE'].max()
                # if mindate == valdate1:
                #     file.write("\n   min date:{} | Status:PASS | Expected: min date = {} ".format(mindate, valdate1))
                #
                # else:
                #     file.write("\n   min date:{} | Status:Failed* | Expected: min date  {}".format(mindate, valdate1))
                #
                # if maxdate == valdate2:
                #     file.write("\n   max date:{} | Status:PASS | Expected: max date = {} ".format(maxdate, valdate2))
                #
                # else:
                #     file.write("\n   max date:{} | Status:Failed* | Expected: max date  {}".format(maxdate, valdate2))
                file.write("\n Date Range:From {} to {} ".format(date1, date2))
                file.write("\n-------------------------------------------------------------")



            except Exception as e:
                print(fltype, "check date format in file", e)

            file.write('\nfinancial RowCount and DataTypes:\n')
            df_count = df_Resf.count().reset_index().rename(columns={'index': 'Fields', 0: ' Row_Count'})
            df_dtype = df_Resf.dtypes.reset_index().rename(columns={'index': 'Fields', 0: ' data_type'})
            df_null_count = df_Resf.isna().sum().reset_index().rename(columns={'index': 'Fields', 0: 'null_count'})
            df_pipefile_schema = df_count.merge(df_dtype.merge(df_null_count, on='Fields', how='left'))
            file.write(df_pipefile_schema.to_string())
            file.write("\n------------------------------------------------------------------------------")

            try:

                NET_ROOM_REVENUE_sum=df_Resf['NET_ROOM_REVENUE'].sum()
                file.write("\n Reservation Future File revenue column sum:")
                file.write("\n NET_ROOM_REVENUE column sum:{}".format(NET_ROOM_REVENUE_sum))

                if pd.to_numeric(df_Resf['NET_ROOM_REVENUE'], errors='coerce').notnull().all() == True :
                    file.write("\n\n Reservation_Future  file NET_ROOM_REVENUE column is numeric \n\n")
                else:file.write(" \n\n Reservation_Future  file NET_ROOM_REVENUE column is not numeric \n\n ")

                if (sum(df_Resf['NET_ROOM_REVENUE'])>0) :
                    pass
                else:print("NET_ROOM_REVENUE sum is not greater than zero")

            except:
                ROOM_REVENUE_sum = df_Resf['ROOM_REVENUE'].sum()
                file.write("\n Reservation Future File revenue column sum:")
                file.write("\n ROOM_REVENUE column sum:{}".format(ROOM_REVENUE_sum))
                if pd.to_numeric(df_Resf['ROOM_REVENUE'], errors='coerce').notnull().all() == True :
                    file.write("\n\n Reservation_Future  file ROOM_REVENUE column is numeric \n\n")
                else:file.write("\n\n Reservation_Future  file ROOM_REVENUE column is not numeric \n\n")

                if (sum(df_Resf['ROOM_REVENUE'])>0) :
                   pass
                else:file.write("\nROOM_REVENUE sum is not greater than zero")
            file.write("\n-------------------------------------------------------------------------------")


            ms_code = ['AIC', 'AIL', 'AIT', 'CMP', 'HSU', 'RFP', 'NRF', 'NRD', 'LST', 'GCR', 'SER', 'INC', 'DLG', 'GLW',
                       'TRR',
                       'TRD', 'TRN', 'PKG', 'PKD', 'TAC', 'TCD', 'TAF', 'TLD', 'GLR', 'GLD', 'CHD', 'ZZZ', 'FIT']
            ms_code_file = list(df_Resf['MARKET_CODE'])

            if set(ms_code_file).issubset(ms_code) == True:
                file.write("\n all ms code are present")
            else:
                a = set(ms_code_file).difference(set(ms_code))

                file.write("\nNew mscode are Found in reservation future file {}\n".format(a))

            file.write("\n==============================================================================")

        elif fltype == 'Group':
            try:
                df_gu = pd.read_csv(filename, sep="|", index_col=False,   error_bad_lines=False, encoding="utf-8")
            except:
                df_gu = pd.read_csv(filename, sep="|", index_col=False,   error_bad_lines=False, encoding="utf-8")
            colnum = df_gu.shape[1]
            maxrows = df_gu.shape[0]

            file.write("\nFile Name:{}".format(filename))
            file.write("\n File Type:{}".format(fltype))

            std_col = ['ALLOTMENT_CODE','BLOCK_NAME','RATE_CODE','BOOKING_STATUS','BEGIN_DATE','COMPANY_NAME_ID','AGENT_NAME_ID','MARKET_CODE',
            'CHANNEL','SOURCE','MIN_RATE1','MIN_RATE2','ALLOTMENT_DATE','ROOM_TYPE','ALLOTED','PICKUP','AVAIL','CREATED_DATE']
            fileCols = (list(df_gu.columns))


            if colnum == 1:

                file.write('\n ============================================================================= \n')
                file.write("\n### Plz Check {} file is NOT in pipe Separated ###\n".format(filename))
                file.write('\n ============================================================================= \n')
                print("\n### Plz Check {} file is NOT in pipe Separated ###\n".format(filename))
                return ('NA')
            else:
                file.write(" \n {} : Pipe Separated and has {} columns ".format(fltype,colnum))

            if set(std_col).issubset(fileCols) == True:
                file.write("\nAll Columns are present in group file\n")
            else:
                a = set(std_col).difference(set(fileCols))
                file.write("\nAll Columns are  NOT in group file {} \t filename:{}\n".format(a,filename))
                file.write("\nNew Columns are  Found in group file {}\n".format(a))


            file.write('\n max_row count of dataframe:{}'.format(str(maxrows)))

            file.write("\n-------------------------------------------------------------------------------")

            try:
                df_gu['BEGIN_DATE'] = pd.to_datetime(df_gu['BEGIN_DATE'], format='%d-%b-%y')
                df_gu['ALLOTMENT_DATE'] = pd.to_datetime(df_gu['ALLOTMENT_DATE'], format='%d-%b-%y')
                df_gu['CREATED_DATE'] = pd.to_datetime(df_gu['CREATED_DATE'], format='%d-%b-%y')

                date1 = df_gu['ALLOTMENT_DATE'].min().strftime('%d-%b-%y')
                date2 = df_gu['ALLOTMENT_DATE'].max().strftime('%d-%b-%y')

                # xDate = datetime.today()
                # valdate1 = xDate
                # valdate2 = xDate + timedelta(days=365)
                # mindate = df_gu['ALLOTMENT_DATE'].min()
                # maxdate = df_gu['ALLOTMENT_DATE'].max()
                # if mindate == valdate1:
                #     file.write("\n   min date:{} | Status:PASS | Expected: min date = {} ".format(mindate, valdate1))
                #
                # else:
                #     file.write("\n   min date:{} | Status:Failed* | Expected: min date  {}".format(mindate, valdate1))
                #
                # if maxdate == valdate2:
                #     file.write("\n   max date:{} | Status:PASS | Expected: max date = {} ".format(maxdate, valdate2))
                #
                # else:
                #     file.write("\n   max date:{} | Status:Failed* | Expected: max date  {}".format(maxdate, valdate2))

                file.write("\n Date Range:From {} to {} ".format(date1,date2))

                file.write("\n-----------------------------------------------------------------------------")
            except Exception as e:
                print(fltype, "check date format in GROUP file", e)


            file.write('\n Group RowCount and DataTypes:\n')
            df_count = df_gu.count().reset_index().rename(columns={'index': 'Fields', 0: ' Row_Count'})
            df_dtype = df_gu.dtypes.reset_index().rename(columns={'index': 'Fields', 0: ' data_type'})
            df_null_count = df_gu.isna().sum().reset_index().rename(columns={'index': 'Fields', 0: 'null_count'})
            df_pipefile_schema = df_count.merge(df_dtype.merge(df_null_count, on='Fields', how='left'))
            file.write(df_pipefile_schema.to_string())
            file.write("\n------------------------------------------------------------------------------")
            try:
                minrate1_sum=df_gu['MIN_RATE1'].sum()
                minrate2_sum=df_gu['MIN_RATE2'].sum()

                file.write("\n  Group revenue columns sum")
                file.write("\n MIN_RATE1 column sum:{}".format(minrate1_sum))
                file.write("\n MIN_RATE2 column sum:{}".format(minrate2_sum))

            except Exception as e:
                file.write("\nError key mesg:{}".format(e))

            file.write("\n==============================================================================")
        else:
            pass
    else:
        print(f"\n#########{fltype} not received for {cl_htl}#######\n")
        file.write("\n==============================================================================")
    return file












