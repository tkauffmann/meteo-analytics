import requests
import time

def get_headers_data(apikey):
    headers = {
        "apikey": apikey,
    }
    data = {
        'accept': '*/*'
    }
    return headers, data


def get_list_of_stations(headers, data):
    url_station = "https://public-api.meteofrance.fr/public/DPClim/v1/liste-stations/horaire?id-departement=75"
    response_station = requests.get(url_station, headers=headers, data=data)
    print(response_station)

    # Check if the request was successful
    if response_station.status_code == 200:
        # Parse the JSON response
        result = response_station.json()
        print("API Response station:", result)
    else:
        print(f"Error: {response_station.status_code}")
    return result

def request_data(headers, data, station_number=75114001):
    url_data = f"https://public-api.meteofrance.fr/public/DPClim/v1/commande-station/horaire?id-station={station_number}&date-deb-periode=2024-01-01T00%3A00%3A00Z&date-fin-periode=2024-01-02T00%3A00%3A00Z"
    response_data = requests.get(url_data, headers=headers, data=data)

    if response_data.status_code == 200:
        print(response_data.json())
    elif response_data.status_code == 202:
        print("requested")
    else:
        print(f"Error: {response_data.status_code}")
        raise(Exception("Error"))

    number = response_data.json()["elaboreProduitAvecDemandeResponse"]["return"]
    return number

def download(headers, data, number):
    url_donwload = f"https://public-api.meteofrance.fr/public/DPClim/v1/commande/fichier?id-cmde={number}"

    i = 0
    not_ready = True
    while not_ready:
        response_donwload = requests.get(url_donwload, headers=headers, data=data)
        if response_donwload.status_code == 204:
            print("not ready")
        else:
            print(response_donwload)
            print(response_donwload.headers)
            file_name = response_donwload.headers.get('Content-Disposition').split('filename=')[1].strip('"')

            # Save the file locally
            with open(file_name, 'wb') as file:
                file.write(response_donwload.content)

            print(f"File downloaded successfully as {file_name}")

            not_ready = False
        time.sleep(1)
        if i == 100:
            print("timeout")
            break
        i += 1
    return file_name

def process_data(file_name):
    import pandas as pd
    df = pd.read_csv(file_name, sep=';')
    print(df.head())
    print(df.columns)
    print(df.dtypes)
    print(df.describe())
    print(df.isnull().sum())
    print(df.shape)


if __name__ == "__main__":
    apikey = "eyJ4NXQiOiJZV0kxTTJZNE1qWTNOemsyTkRZeU5XTTRPV014TXpjek1UVmhNbU14T1RSa09ETXlOVEE0Tnc9PSIsImtpZCI6ImdhdGV3YXlfY2VydGlmaWNhdGVfYWxpYXMiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJwaXN0b3VpbGxlQGNhcmJvbi5zdXBlciIsImFwcGxpY2F0aW9uIjp7Im93bmVyIjoicGlzdG91aWxsZSIsInRpZXJRdW90YVR5cGUiOm51bGwsInRpZXIiOiJVbmxpbWl0ZWQiLCJuYW1lIjoiRGVmYXVsdEFwcGxpY2F0aW9uIiwiaWQiOjE4NTY5LCJ1dWlkIjoiYzIxMzNlYmItZjBkNy00OGZhLTg5NDUtMDg1ZTlhMmY2M2NmIn0sImlzcyI6Imh0dHBzOlwvXC9wb3J0YWlsLWFwaS5tZXRlb2ZyYW5jZS5mcjo0NDNcL29hdXRoMlwvdG9rZW4iLCJ0aWVySW5mbyI6eyI1MFBlck1pbiI6eyJ0aWVyUXVvdGFUeXBlIjoicmVxdWVzdENvdW50IiwiZ3JhcGhRTE1heENvbXBsZXhpdHkiOjAsImdyYXBoUUxNYXhEZXB0aCI6MCwic3RvcE9uUXVvdGFSZWFjaCI6dHJ1ZSwic3Bpa2VBcnJlc3RMaW1pdCI6MCwic3Bpa2VBcnJlc3RVbml0Ijoic2VjIn19LCJrZXl0eXBlIjoiUFJPRFVDVElPTiIsInN1YnNjcmliZWRBUElzIjpbeyJzdWJzY3JpYmVyVGVuYW50RG9tYWluIjoiY2FyYm9uLnN1cGVyIiwibmFtZSI6IkRvbm5lZXNQdWJsaXF1ZXNDbGltYXRvbG9naWUiLCJjb250ZXh0IjoiXC9wdWJsaWNcL0RQQ2xpbVwvdjEiLCJwdWJsaXNoZXIiOiJhZG1pbl9tZiIsInZlcnNpb24iOiJ2MSIsInN1YnNjcmlwdGlvblRpZXIiOiI1MFBlck1pbiJ9XSwiZXhwIjoxODIyNzkzNTAzLCJ0b2tlbl90eXBlIjoiYXBpS2V5IiwiaWF0IjoxNzI4MTIwNzAzLCJqdGkiOiI1NjU0MjNhMS1lNzQwLTRjZjEtYTZkNy1iZTU2NTQwOWUwODgifQ==.VFz4Fnel6RGROU8YSDUubgNc7xuLgP08ZspYJM2Va8Vn79L7JAgDzlkDs6rq21CVxm_Evk5P5SJPZ3fcPzbh5TEoWvquTzrJOLPnAUsmMRigksAgLcW0FYb18e4TFhXtcrStJ3PkH44cAdN84pBlT2kKw3DVsJzbZM9gvjXUHav1qt-r-mAAd0aes9htqHxqKDAZBZIcE6pEeayTh6HHVp5xcbAzRhgS_9CSTuYJfAHF-Nw_3RupyuMe8RDjxW3kdVhOOJB9so1omWfsEVzc_hoxJXLO9CxeJ7o1yzkmDbBXlIv6e96D4fJU_4qdnir09vPGHnn20RkUaBxqaO3R6A=="
    headers, data = get_headers_data(apikey)
    # result = get_list_of_stations(headers, data) -not needed to run every time, I just found that Montsouris station is number 75114001
    number = request_data(headers, data, station_number=75114001)
    file_name = download(headers, data, number)
    process_data(file_name)