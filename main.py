import os
import zipfile
import aiohttp
import asyncio

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

async def download_and_extract(uri):
    async with aiohttp.ClientSession() as session:
        async with session.get(uri) as response:
            if response.status == 200:
                file_name = os.path.basename(uri)
                zip_file_path = os.path.join("downloads", file_name)

                with open(zip_file_path, 'wb') as zip_file:
                    zip_file.write(await response.read())

                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    zip_ref.extractall("downloads")

                os.remove(zip_file_path)
                print(f"Downloaded and extracted {file_name}")
            else:
                print(f"Failed to download {uri}")


def main():
    if not os.path.exists("downloads"):
      os.makedirs("downloads")

    loop = asyncio.get_event_loop()
    tasks = [download_and_extract(uri) for uri in download_uris]
    loop.run_until_complete(asyncio.gather(*tasks))


if __name__ == "__main__":
    main()
