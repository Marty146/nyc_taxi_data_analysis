import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
pd.set_option('display.max_columns', None)

def read_raw_data():
    """
    INPUT DATASET (CSV):

    VendorID:               A code indicating the TPEP provider that provided the record.
                            1= Creative Mobile Technologies, LLC; 2= VeriFone Inc.
    tpep_pickup_datetime:   The date and time when the meter was engaged.
    pep_dropoff_datetime:   The date and time when the meter was disengaged.
    Passenger_count:        The number of passengers in the vehicle.    This is a driver-entered value.
    Trip_distance:          The elapsed trip distance in miles reported by the taximeter.
    PULocationID:           TLC Taxi Zone in which the taximeter was engaged
    DOLocationID:           TLC Taxi Zone in which the taximeter was disengaged
    RateCodeID:             The final rate code in effect at the end of the trip.
                            1= Standard rate
                            2=JFK
                            3=Newark
                            4=Nassau or Westchester
                            5=Negotiated fare
                            6=Group ride
    Store_and_fwd_flag:     This flag indicates whether the trip record was held in vehicle
                            memory before sending to the vendor, aka “store and forward,”
                            because the vehicle did not have a connection to the server.
                            Y= store and forward trip
                            N= not a store and forward trip
    Payment_type:           A numeric code signifying how the passenger paid for the trip.
                            1= Credit card
                            2= Cash
                            3= No charge
                            4= Dispute
                            5= Unknown
                            6= Voided trip
    Fare_amount:            The time-and-distance fare calculated by the meter.
    Extra:                  Miscellaneous extras and surcharges.  Currently, this only includes
                            the $0.50 and $1 rush hour and overnight charges.
    MTA_tax:                $0.50 MTA tax that is automatically triggered based on the metered
                            rate in use.
    Improvement_surcharge:  $0.30 improvement surcharge assessed trips at the flag drop. The
                            improvement surcharge began being levied in 2015.
    Tip_amount:             Tip amount – This field is automatically populated for credit card
                            tips. Cash tips are not included.
    Tolls_amount:           Total amount of all tolls paid in trip.
    total_amount:           The total amount charged to passengers. Does not include cash tips.
    """
    return pd.read_csv('yellow_tripdata_2020-12.csv')

def get_list_of_taxi_zones_in_borough(borough):

    """
    :param borough (String): Name of Borough
    :return (List): List with all IDs in the given borough
    """

    all_taxi_zones=pd.read_csv('taxi+_zone_lookup.csv')
    searched_taxi_zones=[]
    for index,row in all_taxi_zones.iterrows():
        if(row['Borough']==borough):
            searched_taxi_zones.append(row['LocationID'])
    return searched_taxi_zones

def get_avg_price_of_data(dataframe):

    """
    :param dataframe (Dataframe): filtered or raw Dataset of TLC Trip Record Data
    :return (Float): average Price charged to the Customer wthin the given Dataset
    """

    amount_trips=len(dataframe)
    sum=0
    for index,row in (dataframe.iterrows()):
        sum+=row['total_amount']
    return round(sum/amount_trips,2)

def extract_taxi_zone_id_by_zone_name(zone_name):

    """
    :param zone_name (String): Name of Taxi zone
    :return (Integer): Number of Taxi zone
    """

    all_taxi_zones = pd.read_csv('taxi+_zone_lookup.csv')
    relevant_zone_info=all_taxi_zones.loc[all_taxi_zones['Zone']==zone_name]
    return relevant_zone_info['LocationID'].iloc[0]

def calculate_date_delta(date1,date2):

    """
    Calculated Time Difference between two different Dates

    :param date1 (String): Start Date of Time Span
    :param date2 (String): End Time of Time Span
    :return (Integer): Duration of Trip in Minutes
    """

    date_format_str = '%Y-%m-%d %H:%M:%S'
    start = datetime.strptime(date1, date_format_str)
    end = datetime.strptime(date2, date_format_str)
    diff=(end-start).total_seconds() / 60.0
    return diff

def get_hour_of_date(date):
    """
    :param date (String): Date
    :return (Integer): Hour of Date
    """

    date_format_str = '%Y-%m-%d %H:%M:%S'
    relevant_date = datetime.strptime(date, date_format_str)
    return relevant_date.hour


def task_1():
    # JFK Airport Taxi Zone =132
    manhattan_taxi_zones=get_list_of_taxi_zones_in_borough('Manhattan')
    raw_data=read_raw_data()

    # filter DF with trips from Manhattan to JFK
    data_manhattan_to_jfk=raw_data.loc[(raw_data['DOLocationID']==132) & (raw_data['PULocationID'].isin(manhattan_taxi_zones))]

    # Write DF to parquet-File
    data_manhattan_to_jfk.to_parquet('manhattan_to_jfk.parquet', engine='fastparquet')

    # Solutions to Questions from Task1
    print("Total NYC-Taxi Trips (December): {}".format(len(raw_data)))
    print("Total NYC-Taxi Trips from Manhattan to JFK (December): {}".format(len(data_manhattan_to_jfk)))
    print("Average Price of Trip Manhattan to JFK: {}".format(get_avg_price_of_data(data_manhattan_to_jfk))+" $")

    # Other interesting Features
    # returns array of values of payment type: 1.value: Credit Card, 2.Value: Cash, 3.Value: No Charge, 4.Value: Dispute
    payment_counts=data_manhattan_to_jfk['payment_type'].value_counts().values.tolist()

    # no information provided for Payment Type
    payment_counts_no_specification=data_manhattan_to_jfk['payment_type'].isna().sum()

    # Plotting Distribution of Payment Types
    payment_type=['Credit Card','Cash','No Charge','Dispute','No Specification']
    payment_counts.append(payment_counts_no_specification)
    fig1=plt.figure(1)
    plt.bar(payment_type, payment_counts)
    plt.title('Distribution of payment types')
    plt.xlabel('Payment Type')
    plt.ylabel('Count')

    # Invetigating Store and Forward behaviour
    data_manhattan_to_jfk_with_no_connection=data_manhattan_to_jfk.loc[data_manhattan_to_jfk['store_and_fwd_flag']=='Y']
    store_and_fwd_counts_start = data_manhattan_to_jfk_with_no_connection['PULocationID'].value_counts()
    store_and_fwd_counts_end = data_manhattan_to_jfk_with_no_connection['DOLocationID'].value_counts()

    # Plotting Store and Forward Distribution (Start,End) according to taxi zone
    fig2=plt.figure(2)
    plt.subplot(1, 2, 1)
    plt.title("Store and forward-Trips  (Start)")
    plt.bar(store_and_fwd_counts_start.index.tolist(),store_and_fwd_counts_start.values.tolist())
    plt.xlabel("Taxi Zones")
    plt.ylabel("Count")

    plt.subplot(1,2,2)
    plt.title("Store and forward-Trips  (End)")
    plt.bar(store_and_fwd_counts_end.index.tolist(),store_and_fwd_counts_end.values.tolist())
    plt.xlabel("Taxi Zones")
    plt.ylabel("Count")
    plt.figtext(0.5, 0.03, "Connection Errors to Server in Taxi Zone 132", ha="center", fontsize=5,bbox={"facecolor": "orange", "alpha": 0.5, "pad": 5})

    plt.show()



def task_2():
    ########## Task 2.1 ##########

    brooklyn_heights_id=extract_taxi_zone_id_by_zone_name("Brooklyn Heights")
    empire_state_building_location_id= 164  # Research in WWW
    raw_data = read_raw_data()
    data_brooklyn_heigh_to_empire_state = raw_data.loc[(raw_data['PULocationID']==brooklyn_heights_id) & (raw_data['DOLocationID'] == empire_state_building_location_id) ]

    print("Total Trips from Brooklyn Heights to Empire State Building: {}".format(len(data_brooklyn_heigh_to_empire_state)))

    ########## Task 2.2 ##########

    # calculate trip duration and append it a new columnn to DF "data_brookly_heigh_to_empire_state
    data_brooklyn_heigh_to_empire_state['trip_duration'] = data_brooklyn_heigh_to_empire_state.apply(lambda row: calculate_date_delta(row.tpep_pickup_datetime,row.tpep_dropoff_datetime), axis=1)

    # calculate avg trip duration
    sum_trip_duration=0
    for index,row in data_brooklyn_heigh_to_empire_state.iterrows():
        sum_trip_duration+=row['trip_duration']
    avg_trip_duration=sum_trip_duration/len(data_brooklyn_heigh_to_empire_state)
    print("Average Duration from Brooklyn Heights to Empire State Building: {} Minutes".format(round(avg_trip_duration,2)))

    ########## Task 2.3 ##########

    print("Minimum trip amount charged to customer: {} $".format(data_brooklyn_heigh_to_empire_state['total_amount'].min()))
    print("Maximum trip amount charged to customer: {} $".format(data_brooklyn_heigh_to_empire_state['total_amount'].max()))

    ########## Task 2.4 ##########
    # Surcharges = Extra, MTA_tax, Improvment_surcharges
    # add average surcharge as new column to DF
    # average_surcharge = ( mta_tax+extra+improvment_charge+ (total_amount-fare_amount) ) / (trip_duration/60)
    data_brooklyn_heigh_to_empire_state['avg_surcharge_per_hour'] = data_brooklyn_heigh_to_empire_state.apply(lambda row: ( (row.extra+row.mta_tax+row.improvement_surcharge+(row.total_amount-row.fare_amount))/(row.trip_duration/60) ), axis=1)

    # calculate avg surcharge over entire Dataset
    avg_surcharge_per_hour_total=0
    for index,row in data_brooklyn_heigh_to_empire_state.iterrows():
        avg_surcharge_per_hour_total+=row['avg_surcharge_per_hour']
    avg_surcharge_per_hour_total =round(avg_surcharge_per_hour_total/len(data_brooklyn_heigh_to_empire_state),2)
    print("Average Surcharge per Hour: {} $".format(avg_surcharge_per_hour_total))

    ######### Task 2.5 ###########
    # add start_hour and end_hour as new column to DF
    data_brooklyn_heigh_to_empire_state['start_hour']=data_brooklyn_heigh_to_empire_state.apply(lambda row: get_hour_of_date(row.tpep_pickup_datetime),axis=1)
    data_brooklyn_heigh_to_empire_state['end_hour']=data_brooklyn_heigh_to_empire_state.apply(lambda row: get_hour_of_date(row.tpep_dropoff_datetime),axis=1)

    # filter DF according task instructions (9AM-6PM)
    data_brooklyn_heigh_to_empire_state=data_brooklyn_heigh_to_empire_state.loc[(data_brooklyn_heigh_to_empire_state['start_hour']>=9) & (data_brooklyn_heigh_to_empire_state['end_hour']<=18)]

    # Best Hour Regarading Trip Duration
    data_min_trip_duration=data_brooklyn_heigh_to_empire_state.loc[data_brooklyn_heigh_to_empire_state['trip_duration']==data_brooklyn_heigh_to_empire_state['trip_duration'].min()]
    print("Best Time to take trip from Brooklyn Heights to Empire State Bulding regarding trip duration: {} ({} Min)".format(data_min_trip_duration['tpep_pickup_datetime'].iloc[0],data_min_trip_duration['trip_duration'].iloc[0]))

   # Best Hour Regarding Pricing
    data_min_trip_pricing = data_brooklyn_heigh_to_empire_state.loc[data_brooklyn_heigh_to_empire_state['total_amount'] == data_brooklyn_heigh_to_empire_state['total_amount'].min()]
    print("Best Time to take trip from Brooklyn Heights to Empire State Bulding regarding trip Pricing: {} ({} $)".format(data_min_trip_pricing['tpep_pickup_datetime'].iloc[0], data_min_trip_pricing['total_amount'].iloc[0]))

if __name__ == '__main__':


    input=int(input("Which task would you like to evaluate (Possible Values: [1,2]) :  "))
    if input==1:
        task_1()
    elif input==2:
        task_2()
    else:
        print("Wrong Number entered, please select '1' or '2' ")



