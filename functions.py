import pandas as pd
import os
import re


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
        df.drop(df.index[0], inplace = True)
        cur_period_fund_names = set(df['Fund'].unique())

        #list of funds that are absent in current month transactions, but which we need to track 
        diff_list = list(fund_names - cur_period_fund_names)

        #add those extra rows where necessary
        prc_dt = df['Unit Price Date'][1]
        for fnd in diff_list:
            #print (prc_dt, fnd)
            new_row = {'Fund':fnd, 'Fund Currency':'GBP', 'Units':0, 'Unit Price Date': prc_dt, 'Value': 0}
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
    prices_data.reset_index(drop=True, inplace=True)        

    prices_data = clean_prices(prices_data)

    return prices_data

    
def clean_names(name): 
    """
    Cleans the difference in names on transactions and historical prices datasets
    """
    
    # Search for opening bracket in the name followed by 
    # any characters repeated any number of times 
    
    if re.search('\(V.*', name): 
  
        # Extract the position of beginning of pattern 
        pos = re.search('\(V.*', name).start() 
        
        # return the cleaned name 
        return name[:pos-1]       
   
    else: 
        # if clean up needed return the same name
    
        return name


def clean_numbers(numb):
    #numb = re.sub("[^\d\.]", "", str(numb))    
    numb = re.sub("([0-9]{1,3}(,[0-9]{3})*(\.[0-9]+)?|\.[0-9]+)", "", str(numb))
    #numb = float(numb)
    return numb


def clean_prices(prices):
    
    """
    Cleans the difference between find names with historical prices and fund names as shown in transactions
    Quick & dirty for now...
    """
    
    prices['Fund Name'] = prices['Fund Name'].apply(clean_names)
    
    #quick & dirty fix of custom names as i am lazy today...
    
    prices["Fund Name"] = prices["Fund Name"].str.replace('UBS Global AM ','')
    prices["Fund Name"] = prices["Fund Name"].str.replace('Global Equity','Global Equity Fund')
    prices["Fund Name"] = prices["Fund Name"].str.replace('Lifestyle','Growth Fund (Annuity Lifestyle)')
    prices["Fund Name"] = prices["Fund Name"].str.replace('USA','North America')
    prices["Fund Name"] = prices["Fund Name"].str.replace('Emerging Markets Equity','Emerging Markets Equity Fund')
    prices["Fund Name"] = prices["Fund Name"].str.replace('Tracker','Tracker Fund')
    prices["Fund Name"] = prices["Fund Name"].str.replace('HSBC Amanah Global Equity Fund Index Fund','HSBC Amanah Global Equity Index Fund')
    prices["Fund Name"] = prices["Fund Name"].str.replace('World (ex-UK) Equity Tracker Fund Fund','World (ex-UK) Equity Tracker Fund')
    prices["Fund Name"] = prices["Fund Name"].str.replace('Sustainable Global Equity Fund Fund',
                                                          'Sustainable Global Equity Fund' )

                             
    return prices

def cumul_volumes(df):
    """
    Function creates cumulative counts of units & dollars per instrument
    """
    vlist = (df['Fund'].unique())

    new_df = pd.DataFrame(columns = df.columns)
    new_df['UnitsCumul'] = 0
    new_df['ValueCumul'] = 0
    for e in vlist:
        cur_df = df[df['Fund'] == e]
        cur_df['UnitsCumul'] = round(cur_df['Units'].cumsum(),2)
        cur_df['ValueCumul'] = round(cur_df['Value'].cumsum(),2)
        new_df = pd.concat([new_df, cur_df])
        new_df.sort_values(by='Unit Price Date', ascending = True, inplace=True)
        new_df.reset_index(drop=True, inplace=True)
    return new_df




