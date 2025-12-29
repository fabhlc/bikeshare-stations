import dlt
from dlt.sources.helpers import requests


@dlt.resource(
    write_disposition="merge",
    primary_key=["station_id", "last_reported"])

def station_status():
    system_url = "https://tor.publicbikesystem.net/ube/gbfs/v1/en/station_status"
    response = requests.get(system_url).json()
    yield from response["data"]["stations"]


def load_bikeshare():
    pipeline = dlt.pipeline(
        pipeline_name="bikeshare_dlt",
        destination="motherduck",
        dataset_name="bikeshare",
        progress="log"
    )
    
    load_info = pipeline.run(station_status)
    print(load_info)


if __name__ == "__main__":
    load_bikeshare()