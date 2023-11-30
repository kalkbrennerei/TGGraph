import requests
import os

ACCESS_TOKEN = os.getenv("ZENODO_ACCESS_TOKEN")

r = requests.get('https://zenodo.org/api/records/7640712',
                  params={'access_token': ACCESS_TOKEN})

print(r.status_code)

download_urls = [f['links']['self'] for f in r.json()['files']]

filenames = [f['key'] for f in r.json()['files']]

print(download_urls)

for filename, url in zip(filenames, download_urls):
    print("Downloading:", filename)
    r = requests.get(url, params={'access_token': ACCESS_TOKEN})
    with open(f"/data/tgdataset/{filename}", 'wb') as f:
        f.write(r.content)