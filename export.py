import os
import sys
import logging
from datetime import datetime, timedelta, timezone
from influxdb_client import InfluxDBClient
import configparser
import ast
import csv

# Configure logging
logging.basicConfig(filename='export.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Read the configuration file
config = configparser.ConfigParser()
try:
    if not os.path.exists('config.ini'):
        raise FileNotFoundError("config.ini file not found")
    config.read('config.ini')
except configparser.Error as e:
    logging.error(f"An error occurred while reading the config.ini file: {e}")
    sys.exit(1)

# Extract necessary configuration parameters
my_url = config['INFLUXDB']['url']
my_token = config['INFLUXDB']['token']
my_org = config['INFLUXDB']['org']
my_bucket = config['INFLUXDB']['bucket']
time_range_days = config['INFLUXDB']['time_range_days']
time_range_hours = config['INFLUXDB']['time_range_hours']
my_window_period = config['INFLUXDB']['window_period']

# Check if time_range_days and time_range_hours are not empty
if not time_range_days or not time_range_hours:
    logging.error("The 'time_range_days' and 'time_range_hours' keys must not be empty in the 'INFLUXDB' section of "
                  "the config.ini file.")
    sys.exit(1)

# Convert filtered_fields from string to list of dictionaries
filtered_fields_str = config['INFLUXDB']['filtered_fields']
my_filtered_fields = ast.literal_eval(filtered_fields_str)

# Determine aggregation type
if config['INFLUXDB']['aggregation_type'] == 'raw':
    my_aggregation_type = 'raw'
else:
    my_aggregation_type = 'windowed'

# Initialize InfluxDB client and query API
client = InfluxDBClient(url=my_url, token=my_token, org=my_org)
query_api = client.query_api()


class QueryGenerator:
    """
       A class used to generate InfluxDB queries based on provided parameters.

    ...

   Attributes
   ----------
   ALLOWED_OPERATORS : list
       a list of allowed operators for filter conditions
   query : str
       a string template for the InfluxDB query
   params : dict
       a dictionary of parameters for the InfluxDB query
   filter_conditions : str
       a string of filter conditions for the InfluxDB query
   aggregation_type : str
       the type of aggregation for the InfluxDB query

   Methods
   -------
   add_filter_conditions(conditions)
       Adds filter conditions to the InfluxDB query.
   set_time_range(days, hours)
       Sets the time range for the InfluxDB query.
   set_window_period(window_period)
       Sets the window period for the InfluxDB query.
   set_aggregation_type(aggregation_type)
       Sets the aggregation type for the InfluxDB query.
   generate()
       Generates the InfluxDB query and returns it along with its parameters.
   """

    def __init__(self, bucket):
        """
        Constructs all the necessary attributes for the QueryGenerator object.

        Parameters
        ----------
            bucket : str
                the bucket to query from
        """
        self.ALLOWED_OPERATORS = ['==', '!=', '<', '>', '<=', '>=']
        self.query = '''
                from(bucket: stringParam)
                    |> range(start: timeRangeStart, stop: timeRangeStop)
                    {filter_conditions}
                     {aggregation}
                    '''
        self.params = {
            'stringParam': bucket,
            'timeRangeStart': (datetime.now() - timedelta(days=30)).isoformat(),
            'timeRangeStop': datetime.now().isoformat(),
            'windowPeriod': "1d"
        }
        self.filter_conditions = ''
        self.aggregation_type = 'windowed'

    def add_filter_conditions(self, conditions):
        """
        Adds filter conditions to the InfluxDB query.

        Parameters
        ----------
            conditions : list
                a list of dictionaries, each representing a filter condition
        """
        if isinstance(conditions, dict):
            conditions = [conditions]

        for condition in conditions:
            if not isinstance(condition, dict):
                raise ValueError(f"Invalid condition: {condition}. Must be a dictionary.")
            if 'field' not in condition or 'operator' not in condition or 'value' not in condition:
                raise ValueError(f"Invalid condition: {condition}. Must contain 'field', 'operator', and 'value'.")
            if condition['operator'] not in self.ALLOWED_OPERATORS:
                raise ValueError(f"Invalid operator: {condition['operator']}. Must be one of {self.ALLOWED_OPERATORS}.")

            field = condition['field']
            operator = condition['operator']
            value = condition['value']
            self.filter_conditions += f'|> filter(fn: (r) => r["{field}"] {operator} "{value}")\n'

    def set_time_range(self, days, hours):
        """
        Sets the time range for the InfluxDB query.

        Parameters
        ----------
            days : int
                the number of days for the time range
            hours : int
                the number of hours for the time range
        """
        # check if days and hours are integers and non-negative integers.isdigit() gives error because
        # AttributeError: 'int' object has no attribute 'isdigit'.
        days = int(days)
        hours = int(hours)
        if days < 0 or hours < 0:
            raise ValueError("Days and hours must be non-negative integers.")
        time_delta = timedelta(days=int(days), hours=int(hours))
        self.params['timeRangeStart'] = datetime.now(timezone.utc) - time_delta
        self.params['timeRangeStop'] = datetime.now(timezone.utc)

    def set_window_period(self, window_period):
        """
        Sets the window period for the InfluxDB query.

        Parameters
        ----------
            window_period : str
                the window period for the query
        """
        # check if window_period is one of 's', 'm', 'h', 'd'.
        if window_period[-1] not in ['s', 'm', 'h', 'd']:
            raise ValueError("Window period must end with one of 's', 'm', 'h', 'd'.")
        # check if window_period is positive integer
        if not window_period[:-1].isdigit():
            raise ValueError("Window period must be a positive integer.")
        # convert window_period to seconds
        time_dict = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
        window_period = int(window_period[:-1]) * time_dict[window_period[-1]]
        window_period_duration = timedelta(seconds=window_period)
        self.params['windowPeriod'] = window_period_duration

    def set_aggregation_type(self, aggregation_type):
        """
        Sets the aggregation type for the InfluxDB query.

        Parameters
        ----------
            aggregation_type : str
                the aggregation type for the query
        """
        if aggregation_type not in ['raw', 'windowed']:
            raise ValueError("Aggregation type must be either 'raw' or 'windowed'.")
        self.aggregation_type = aggregation_type

    def generate(self):
        """
        Generates the InfluxDB query and returns it along with its parameters.

        Returns
        -------
            tuple
                a tuple containing the InfluxDB query and its parameters
        """
        if self.aggregation_type == 'windowed':
            aggregation = ('|> aggregateWindow(every: windowPeriod, fn: mean, createEmpty: false)\n|> yield(name: '
                           '"mean")')
        else:  # raw
            aggregation = ''
        return self.query.format(filter_conditions=self.filter_conditions, aggregation=aggregation), self.params


def query_generator():
    """
    Generates an InfluxDB query and its parameters using the QueryGenerator class.

    Returns
    -------
        tuple
            a tuple containing the InfluxDB query and its parameters
    """
    generator = QueryGenerator(my_bucket)
    generator.set_time_range(time_range_days, time_range_hours)
    generator.set_aggregation_type(my_aggregation_type)
    generator.set_window_period(my_window_period)
    generator.add_filter_conditions(my_filtered_fields)
    return generator.generate()


# create a csv file
def export_csv():
    """
    Exports the result of an InfluxDB query to a CSV file.
    """
    query, params = query_generator()
    csv_result = query_api.query_csv(query=query, params=params)
    # Generate the filename
    filename = datetime.now().strftime('%Y-%m-%d_%H-%M-%S.csv')

    # Open the CSV file in write mode
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)

        # Write rows to the CSV file
        for row in csv_result:
            writer.writerow(row)


# create main function
if __name__ == "__main__":
    try:
        export_csv()
        logging.info("Exported to CSV successfully")
    except Exception as e:
        logging.error(f"An error occurred during export: {e}")
