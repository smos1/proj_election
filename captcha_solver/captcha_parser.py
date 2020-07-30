import os
import re
import urllib
import requests
import argparse
from io import BytesIO
from time import sleep
from random import randint

from tqdm import tqdm
import matplotlib.pyplot as plt
from matplotlib import image


def get_captcha(referer):
    """Function for get captcha image.

    Args:
        referer (str): URL where captcha is requested.

    Returns:
        numpy.ndarray: PNG image, the float32 numpy array.
        dict: izbirkomSession cookie for authorization in referer

    """

    domain = urllib.parse.urlparse(referer).netloc
    response = requests.get(f"http://{domain}/captcha-service/image/", headers={"Referer": referer})
    response.raise_for_status()

    captcha = plt.imread(BytesIO(response.content))

    match = re.match(r"(izbirkomSession)=([\w-]+)", response.headers["Set-Cookie"])
    if match is not None:
        cookies = {match.group(1): match.group(2)}

        return captcha, cookies
    else:
        raise RuntimeError("izbirkomSession cookie not found")


def get_images(referer, path, n_images):
    if os.path.exists(path):
        raise RuntimeError(f"path {path} already exists")
    else:
        os.mkdir(path)

        for i in tqdm(range(n_images)):
            captcha, _ = get_captcha(referer)
            image.imsave(os.path.join(path, f"{i}.png"), captcha)

            sleep(randint(1, 100) / 1000)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--referer", type=str, help="url that requests captcha")
    parser.add_argument("--path", type=str, help="directory for saving images")
    parser.add_argument("--n_images", type=int, default=100, help="number of images (default: 100)")

    args = parser.parse_args()

    get_images(args.referer, args.path, args.n_images)


if __name__ == '__main__':
    main()
