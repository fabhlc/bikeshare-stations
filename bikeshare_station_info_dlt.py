import dlt
from dlt.sources.helpers import requests

'''
This script initiates a dlt pipeline to load station information (ID, name, lat, long) from the 
Public Bike System API into the MotherDuck database as schema=bikeshare, table=stations_info.
'''

@dlt.resource(
    write_disposition="merge",
    primary_key=["station_id"])

def stations_info():
    stations_info_url = "https://tor.publicbikesystem.net/ube/gbfs/v1/en/station_information"
    response = requests.get(stations_info_url).json()
    yield from response["data"]["stations"]


def load_bikeshare_station_info():
    pipeline = dlt.pipeline(
        pipeline_name="bikeshare_station_info_dlt",
        destination="motherduck",
        dataset_name="bikeshare",
        progress="log"
    )
    
    load_info = pipeline.run(stations_info)
    print(load_info)


if __name__ == "__main__":
    load_bikeshare_station_info()