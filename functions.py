import pandas as pd
import os
import re

#TODO - Add proper exception handling to all functions

def read_data():

    """
    Function reads and combines all data inputs from all *.csv files from 'data/' directory
    Function expects all inputs to be of same structure
    Having name of fund, owned units and date attributes is essential for charting functions to work correctly at later steps
    """

    #get list of files to read
    file_names = [f for f in os.listdir(os.path.join('data')) if f.endswith('.csv')]

    #get a single dataframe with list of columns we need
    all_data = pd.DataFrame(columns = pd.read_csv(os.path.join('data', file_names[0]), sep=',',
                                                  header='infer', dayfirst=True, cache_dates=False).columns)
    #assign default transaction type as switch for now on empty dataframe
    all_data['TransTp'] = 'Switch'
    all_data['Invested'] = 0

    #read & combine files to single dataframe
    #first we get all fund names with open positions at any point of time
    all_fund_names = pd.DataFrame(columns = pd.read_csv(os.path.join('data', file_names[0]), sep=',',
                                                        header='infer', dayfirst=True, cache_dates=False) .columns)

    for f in file_names:
        df_fund_names = pd.read_csv(os.path.join('data', f), sep= ',', header='infer', thousands=',',
                                    dayfirst=True, cache_dates=False)
        df_fund_names.drop(df_fund_names.index[0], inplace = True)
        all_fund_names = pd.concat([all_fund_names,df_fund_names])
    fund_names = set(all_fund_names['Fund'].unique())

    print ('You have/had investments in these funds: \n', fund_names)

    #second, while reading all files content we will add extra rows with Fund name and 0 investments in a period
    #to historic data to do nice cumulative sums with pandas later
    for f in file_names:
        df = pd.read_csv(os.path.join('data', f), sep=',', header='infer', thousands=',',
                         dayfirst=True, cache_dates=False)

        #while reading monthly file we also want to differentiate between switches & regular transactions
        # differentiate between switches and regular monthly contributions (new money)
        contrib = re.findall(r"[-+]?\d*\.\d+|\d+", df.loc[0][0])  # find all floats in first line of file

        try:
            contrib = float(contrib[0])  # expect exactly one float to be overall monthly pension contrib
            df['TransTp'] = 'Contribution'
            df['Invested'] = df['Value'] #preserving proper split between funds from raw data
        except:
            contrib = 0
            df['TransTp'] = 'Switch'
            df['Invested'] = 0

        df.drop(df.index[0], inplace = True) #drop transaction description value from monthly df
        cur_period_fund_names = set(df['Fund'].unique())

        #list of funds that are absent in current month transactions, but which we need to track
        diff_list = list(fund_names - cur_period_fund_names)

        #add those extra rows where necessary
        prc_dt = df['Unit Price Date'][1]
        for fnd in diff_list:
            #print (prc_dt, fnd)
            new_row = {'Fund':fnd, 'Fund Currency':'GBP', 'Units':0, 'Unit Price Date': prc_dt, 'Value': 0,
                       'TransTp':'Historical', 'Invested':0}
            df = df.append(new_row, ignore_index = True)

        all_data = pd.concat([all_data, df])

    #Sort transactions by date and restore broken index
    all_data["Unit Price Date"] = pd.to_datetime(all_data["Unit Price Date"], dayfirst=True)
    all_data["Unit Price Date"] = all_data['Unit Price Date'].dt.date
    all_data.sort_values(by='Unit Price Date', ascending = True, inplace=True)
    all_data.reset_index(drop=True, inplace=True)

    #all_data_with_cumul = cumul_volumes(all_data)

    return all_data


def read_prices():

    """
    Function reads prices file as *.csv file from 'prices/' directory
    Function expects exactly one file and will return a warning in case of multiple files in target directory 
    Having name of fund, owned units and date attributes is essential for charting functions to work correctly at later steps
    """

    file_names = [f for f in os.listdir(os.path.join('prices')) if f.endswith('.csv')]

    prices_data = pd.DataFrame()

    try:
        if len(file_names) == 1:
            prices_data = pd.read_csv(os.path.join('prices', file_names[0]), sep=',', header='infer', thousands=',',
                                      dayfirst=True, cache_dates=False)
            prices_data.drop(prices_data.index[-1], inplace = True)
        else:
            print("Oops! There are multiple files in 'prices/' directory.  Leave only one file and try again...")
    except:
        print("Oops!  Try again...")

    prices_data["Unit Price Date"] = pd.to_datetime(prices_data["Unit Price Date"], dayfirst=True)
    prices_data["Unit Price Date"] = prices_data['Unit Price Date'].dt.date
    prices_data.sort_values(by='Unit Price Date', ascending = True, inplace=True)
    prices_data.rename(columns={"Fund Name": "Fund"}, inplace=True)
    prices_data.reset_index(drop=True, inplace=True)

    prices_data = remap(prices_data,remap_dict())

    return prices_data


def remap_dict():
    """
    This function reads all the files in maps/ dictionary and prepares
    replacement dictionary, including all found files and necessary renames to be done.
    Any pairs where original_name == name will be dropped from dictionary
    Remapped  attributes are expected to be in a dataframe supplied for remapping
    """

    #create a replacement dictionary for 'Fundfun_name' attribute
    repl_dict = {}

    #get list of files to read
    file_names = [f for f in os.listdir(os.path.join('maps')) if f.endswith('.csv')]

    #get a single dataframe with list of columns we need
    all_data = pd.DataFrame(columns = pd.read_csv(os.path.join('maps', file_names[0]), sep=',',
                                                  header='infer', dayfirst=True, cache_dates=False).columns)

    for f in file_names:
        df = pd.read_csv(os.path.join('maps', f), sep=',', header='infer',
                      dayfirst=True, cache_dates=False)
        all_data = pd.concat([all_data,df])
        key_value_pairs = df[['original_name', 'name']].set_index('original_name')
        repl_dict[df.iloc[0][0]] = key_value_pairs.transpose().to_dict(orient='records')[0]

    #drop any names that are not going to change with replacement
    for k, v in repl_dict.items():
        rm_lst =[]
        for key, value in v.items():
            if key == value:
                rm_lst.append(key)
        for e in rm_lst:
            v.pop(e)

    #print (repl_dict)
    return repl_dict


def remap(df, dict):
        """
        This function take in a dictionary of labels : dict_labels
        and replace the values (previously labelencode) into the string.

        ex: dict_labels = {{'col1':{1:'A',2:'B'}}

        """
        for field, values in dict.items():
            df.replace({field: values}, inplace=True)
        return df

def calc_cumul_volumes(df):
    """
    Function creates cumulative counts of units & dollars per instrument
    """
    #TODO clean up column names for better aggregation and create a dedicated function to calculate performance
    #TODO consider tracking initial investments and switches separately in units
    vlist = (df['Fund'].unique())

    new_df = pd.DataFrame(columns = df.columns)
    new_df['UnitsCumul'] = 0
    new_df['ValueCumul'] = 0
    for e in vlist:
        cur_df = df[df['Fund'] == e]
        cur_df['UnitsCumul'] = round(cur_df['Units'].cumsum(),2)
        cur_df['ValueCumul'] = round(cur_df['Value'].cumsum(),2)
        cur_df['InvCumul'] = round(cur_df['Invested'].cumsum(), 2)
        new_df = pd.concat([new_df, cur_df])
        new_df.sort_values(by='Unit Price Date', ascending = True, inplace=True)
        new_df.reset_index(drop=True, inplace=True)
    return new_df


def read_data_and_prices():

    """
    Draft wrapper function of simplified/unified read of data and prices with majority of necessary processing
    """

    #TODO - recreate daily picture on both prices & transactions, now some prices will be brough forward for 1 month where transaction dates do not have a corresponding day price
    #This change will affect get_all_data, read_data & read_prices fucntions
    alldata = pd.merge(calc_cumul_volumes(read_data()), read_prices(),
                       how='left', left_on=('Unit Price Date', 'Fund'),
                       right_on=('Unit Price Date', 'Fund'), copy=True)
    alldata['Unit Price'] = alldata.groupby('Fund')['Unit Price'].fillna(method='ffill')
    alldata['Unit Price'] = alldata['Unit Price'].fillna(0)
    alldata['CurVal'] = round(alldata['Unit Price'] * alldata['UnitsCumul'],2)

    alldata.drop(['Fund Currency', 'ValueCumul', ' Fund Code', 'Currency code'], axis=1, inplace=True)

    return alldata

def calc_performance(df):
    """
    Function aggregates data by month and calculates basic performance of investment metric
    """

    #TODO consider doing performace calculation inside cumul_volumes() ? unlikely because of aggregations
    dfperf = df.groupby('Unit Price Date')['Unit Price Date', 'Invested', 'InvCumul', 'CurVal'].sum().copy()
    dfperf['Profit'] = dfperf['CurVal'] - dfperf['InvCumul']
    dfperf['Performance'] = round(dfperf['Profit'] / dfperf['InvCumul'],2)
    dfperf.reset_index(inplace=True)
    return dfperf

def chart_prices(df):

    """"
    Draft version of price charting function
    """

    #TODO - consider dual-axes charts for nice comparison stats
    #code below allow to plot dual-axis chart to compare dynamic of current value with performance of some fund (if used on 'data' df)
    #line_chart = df[df['Fund'] == fnd]\
    #    .plot(secondary_y='Unit Price', x='Unit Price Date', kind='line', figsize=(14, 9), title=fnd)

    #TODO add performance charting, e.g. --> line_c = dt.plot(x = 'Unit Price Date', y = 'Profit', kind= 'line', figsize=(14,9))
    #c = dt.plot(x='Unit Price Date',kind='line', figsize=(14,9))
    #primitive line charts for each unique Fund in Prices data
    for fnd in df['Fund'].unique():
        line_chart = df[df['Fund'] == fnd] \
            .plot(x='Unit Price Date', y='Unit Price', kind='line', figsize=(14, 9), title=fnd)

    # mtr = prices[(prices['Unit Price Date'] > datetime.date(2020,12,1)) & \
    #       (prices['Unit Price Date'] < datetime.date(2021,1,1)) & \
    #        (prices['Fund'] == 'UK Equity Tracker Fund')]

    return None

def chart_performance(df):

    """"
    Draft function
    """
    df.drop('Invested', axis=1, inplace = False).plot(secondary_y='Performance', x='Unit Price Date', kind='line', figsize=(14, 9))
    df[['Unit Price Date', 'Performance', 'Invested']].plot(secondary_y='Performance', x='Unit Price Date', kind='line', figsize=(14, 9))

    return None


def chart_investment(df):
    # c = dt.plot(x='Unit Price Date',kind='line', figsize=(14,9))
    # primitive line charts for each unique Fund in Prices data
    for fnd in df['Fund'].unique():
        line_chart = df[df['Fund'] == fnd] \
            .plot(secondary_y='Unit Price', x='Unit Price Date', kind='line', figsize=(14, 9), title=fnd)

        # line_chart = df[df['Fund'] == fnd]\
        # .plot(x='Unit Price Date', y='CurVal', secondary_y='Unit Price', kind='line', figsize=(14, 9), title=fnd)

    # mtr = prices[(prices['Unit Price Date'] > datetime.date(2020,12,1)) & \
    #       (prices['Unit Price Date'] < datetime.date(2021,1,1)) & \
    #        (prices['Fund'] == 'UK Equity Tracker Fund')]

    return None