import pandas as pd


def compare_data():
    df1 = pd.read_csv('/Users/Victoria/PycharmProjects/BTCData/hour_02.csv')
    df2 = pd.read_csv('/Users/Victoria/PycharmProjects/BTCData/hour_03.csv')
    df3 = pd.read_csv('/Users/Victoria/PycharmProjects/BTCData/hour_04.csv')
    df4 = pd.read_csv('/Users/Victoria/PycharmProjects/BTCData/hour_05.csv')
    df5 = pd.read_csv('/Users/Victoria/PycharmProjects/BTCData/hour_06.csv')

    # Extract a particular column (e.g., 'open') from each DataFrame
    price_date = df1['start']
    price_day = df1['day_name']
    column_from_file1 = df1['pricechange']
    column_from_file2 = df2['pricechange']
    column_from_file3 = df3['pricechange']
    column_from_file4 = df4['pricechange']
    column_from_file5 = df5['pricechange']

    # Optionally, you can combine these columns into a new DataFrame
    combined_df = pd.DataFrame({
        'price_date': price_date,
        "price_day": price_day,
        'hour_02': column_from_file1,
        'hour_03': column_from_file2,
        'hour_04': column_from_file3,
        'hour_05': column_from_file4,
        'hour_06': column_from_file5,
    })
    print(combined_df)


if __name__ == '__main__':
    compare_data()
