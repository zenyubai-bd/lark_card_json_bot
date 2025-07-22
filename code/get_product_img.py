import os
from pathlib import Path

from bs4 import BeautifulSoup
import requests
import html5lib

PATH = os.getcwd()
script_dir = Path(__file__).resolve().parent.parent

def download_image(tts_url, filename):
    """
    Download an image from a URL and save it to a file.
    """
    r = requests.get(tts_url)
    soup = BeautifulSoup(r.content, 'html5lib')

    table = soup.find_all('img', attrs={"class": "lazy-img"})
    try:
        img_url = table[0]['data-src']
    except:
        print("No image found")
        filename = script_dir / "assets" / "No_Image_Available.jpg"
        status = False
        return str(filename), status


    response = requests.get(img_url)
    if response.status_code == 200:
        with open(filename, 'wb') as f:
            f.write(response.content)
        print(f"Image saved as {filename}")
        status = True
    else:
        print(f"Failed to download image: {response.status_code}")
        status = False

    return filename, status