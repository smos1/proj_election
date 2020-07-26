import os
import urllib
import requests
import argparse

from time import sleep
from random import randint

from tqdm import tqdm


def get_images(referer, path, n_images):
    if os.path.exists(path):
        raise RuntimeError(f"path {path} already exists")
    else:
        os.mkdir(path)

        for i in tqdm(range(n_images)):
            domain = urllib.parse.urlparse(referer).netloc
            response = requests.get(f"http://{domain}/captcha-service/image/", headers={"Referer": referer})
            response.raise_for_status()

            with open(os.path.join(path, f"{i}.png"), "wb") as f:
                f.write(response.content)

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
