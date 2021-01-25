def read_data():
    
    """
    Function reads and combines all data inputs from all *.csv files from 'data/' directory
    Function expects all inputs to be of same structure
    Having name of fund, owned units and date attributes is essential for charting functions to work correctly at later steps
    """
    
    import pandas as pd
    import os
    
    #get list of files to read

    file_names = [f for f in os.listdir(os.path.join('data')) if f.endswith('.csv')]
    
    #get a single dataframe with list of columns we need
    all_data = pd.DataFrame(columns = pd.read_csv(os.path.join('data', file_names[0]), sep=',', header='infer').columns)
    
    #read & combine files to single dataframe
    
    for f in file_names:
        df = pd.read_csv(os.path.join('data', f), sep=',', header='infer')
        df.drop(df.index[0], inplace = True)
        all_data = pd.concat([all_data, df])
    
    #Sort transactions by date and restore broken index
    
    all_data.sort_values(by='Unit Price Date', inplace=True)
    
    all_data.reset_index(drop=True, inplace=True)    
    
    return all_data


def read_prices():
    
    """
    Function reads prices file as *.csv file from 'prices/' directory
    Function expects exactly one file and will return a warning in case of multiple files in target directory 
    Having name of fund, owned units and date attributes is essential for charting functions to work correctly at later steps
    """
    
    import pandas as pd
    import os
    
    file_names = [f for f in os.listdir(os.path.join('prices')) if f.endswith('.csv')]
    
    prices_data = pd.DataFrame()
    
    try:
        if len(file_names) == 1:
            prices_data = pd.read_csv(os.path.join('prices', file_names[0]), sep=',', header='infer')
            prices_data.drop(prices_data.index[-1], inplace = True)
        else:
            print("Oops! There are multiple files in 'prices/' directory.  Leave only one file and try again...")
    except:
        print("Oops!  Try again...")
    
    return prices_data

    
def clean_names(name): 
    """
    Cleans the difference in names on transactions and historical prices datasets
    """
    
    import re 
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




def clean_data(prices):
    
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
                             
    return prices
