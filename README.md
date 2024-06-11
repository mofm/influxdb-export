# influxdb-export

This script exports data from an InfluxDB database to a CSV file. It reads configuration parameters from a `config.ini` file, generates an InfluxDB query based on these parameters, executes the query, and writes the result to a CSV file.

## Requirements

This script requires Python 3 and the following Python packages:

- influxdb-client[ciso]~=1.43.0

You can install these packages using pip:

```bash
pip install -r requirements.txt
```

## Configuration
The script reads configuration parameters from a config.ini file. Here's an example of what this file might look like:

```ini
[INFLUXDB]
url = http://localhost:8086
token = my-token
org = my-org
bucket = my-bucket
time_range_days = 1
time_range_hours = 0
aggregation_type = raw
window_period = 1h
filtered_fields = field1,field2
```

## Usage
You can run the script using Python 3:

```bash
python export.py
```

The script will generate a CSV file with the results of the InfluxDB query. The filename will be a timestamp in the format YYYY-MM-DD_HH-MM-SS.csv.

## Logging
The script logs its progress to a file named export.log. The log includes timestamps, log levels (INFO, ERROR), and log messages.