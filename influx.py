from influxdb_client import InfluxDBClient
import os

my_token = os.environ.get('INFLUX_TOKEN')
my_org = "ver"

if __name__ == '__main__':
    client = InfluxDBClient(url="http://localhost:8086", token=my_token, org=my_org)
    query_api = client.query_api()

    query = 'from(bucket: "test")\
  |>  range(start: -30d)\
  |> filter(fn: (r) => r["_measurement"] == "mem")\
  |> filter(fn: (r) => r["_field"] == "available" or r["_field"] == "available_percent" or r["_field"] == "total")'

    result = client.query_api().query(org=my_org, query=query)

    results = []
    for table in result:
        for record in table.records:
            results.append((record.get_value(), record.get_field()))

    print(results)
