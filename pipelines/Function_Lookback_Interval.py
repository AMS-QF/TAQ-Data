#followed by preprocess pipeline, after we get clean data
df_clean['Participant_Timestamp_f'] = df_clean['Participant_Timestamp'].apply(lambda t : t.timestamp())#convert timestamp into float format
#create a test dataframe, including the head 500 rows.
test_copy=df_clean[:500].copy()
test_copy = test_copy.sort_values(by=['Participant_Timestamp'])
test_copy
#generate two columns one is trade volume and one is number of trades from the beginning of the dataframe to each row, thorough out the whole dataframe

test_copy['Cumulative_Trade_Volume'] = test_copy['Participant_Timestamp_f'].apply(lambda t:
                                                    sum(test_copy.fillna(0)[test_copy['Participant_Timestamp_f'].between(test_copy['Participant_Timestamp_f'][0], t, inclusive='left')]['Trade_Volume']))

test_copy['Cum_Trades'] = (test_copy.fillna(0)['Trade_Price'] != 0).cumsum()

#define the lookback interval function according to page 11 of paper
#this function returns a dataframe satisfying certain requrements
def backwards(data, T, delta1, delta2, M):
    #input t should be in timestamp format and then change it to float
    T=T.timestamp()
    if M=='calendar':
        time=pd.Series([T,T-delta1,T-delta2])
        #check whether three timestamps are located in the range of the dataset
        if time.between(data['Participant_Timestamp_f'][0],data['Participant_Timestamp_f'][-1],inclusive='both')==[True, True, True]:
            #check if values of delta1 and delta2 are valid
            if delta1 > delta2:
                return print('Invalid input value of delta1 and delta2')
            else:
                #return part of the dataframe that located in the time interval for further processing
                backward_window=data[data['Participant_Timestamp_f'].between(T-delta1,T-delta2,inclusive='right')]
        else:
            return print('Invalid Time Input')
    
    if M=='transaction':
        time=pd.Series([T])
        #check whether the input timestamp are located in the range of the dataset
        if time.between(df_clean['Participant_Timestamp_f'][0],df_clean['Participant_Timestamp_f'][-1],inclusive='both')==True:
            #cut off the data later than the input timestamp
            filtered_data = data[data['Participant_Timestamp_f'] <= T]
            #check whether the input numbe of transactions are integers
            if delta1.is_integer==True & delta2.is_integer==True:
                #check whether the input number of transactions exceeds the largest possible value till the input timestamp T
                if delta1 & delta2 <= filtered_data['Cum_Trades'][-1]:
                    if delta1 > delta2:
                        return print('Invalid input value of delta1 and delta2')
                    #generate a dataframe including all timestamps t such that number of transactions among (t, T] are between delta1 and delta2 (quote timestamps included)
                    else:
                        backward_window=data[data['Cum_Trades'].between(filtered_data['Cum_Trades'][-1]-delta1,filtered_data['Cum_Trades'][-1]-delta2,inclusive='right')]
                else:
                    return print('Invalid input value of delta1 and delta2')
            else:
                return print('Please Input delta1 and delta2 as Integers')
        else:
            return print('Invalid Time Input')
    
    if M=='volume':
        time=pd.Series([T])
        #check whether the input timestamp are located in the range of the dataset
        if time.between(df_clean['Participant_Timestamp_f'][0],df_clean['Participant_Timestamp_f'][-1],inclusive='both')==True:
            filtered_data = data[data['Participant_Timestamp_f'] <= T]
            #check whether the input number of trade volume exceeds the largest possible value till the input timestamp T
            if delta1 & delta2 <= filtered_data['Cumulative_Trade_Volume'][-1]:
                if delta1 > delta2:
                    return print('Invalid input value of delta1 and delta2')
                #generate a dataframe including all timestamps t such that trade volume among (t, T] are between delta1 and delta2 (quote timestamps included)
                else:
                    backward_window=data[data['Cumulative_Trade_Volume'].between(filtered_data['Cumulative_Trade_Volume'][-1]-delta1,filtered_data['Cumulative_Trade_Volume'][-1]-delta2,inclusive='right')]
            else:
                return print('Invalid input value of delta1 and delta2')
        else:
            return print('Invalid Time Input')