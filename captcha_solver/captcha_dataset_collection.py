import argparse
import base64
import os

from selenium import webdriver
from tqdm import tqdm
from utils import DATA_PATH


class CaptchaBot:
    def __init__(self, data_path=DATA_PATH):
        self.driver = webdriver.Chrome()
        self.link = """http://www.kaliningrad.vybory.izbirkom.ru/region/kaliningrad?action=show&global=true&root=394011014&tvd=4394011224092&vrn=100100163596966&prver=0&pronetvd=null&region=39&sub_region=39&type=232&vibid=4394011224092"""
        self.driver.get(self.link)
        self.image_count = 0
        self.data_path = data_path

    def download_captcha(self):
        ele_captcha = self.driver.find_element_by_xpath('//*[@id="captchaImg"]')
        img_captcha_base64 = self.driver.execute_async_script("""
            var ele = arguments[0], callback = arguments[1];
            ele.addEventListener('load', function fn(){
              ele.removeEventListener('load', fn, false);
              var cnv = document.createElement('canvas');
              cnv.width = this.width; cnv.height = this.height;
              cnv.getContext('2d').drawImage(this, 0, 0);
              callback(cnv.toDataURL('image/png').substring(22));
            }, false);
            ele.dispatchEvent(new Event('load'));
            """, ele_captcha)

        with open(os.path.join(self.data_path, f'{self.image_count}.png'), 'wb') as f:
            f.write(base64.b64decode(img_captcha_base64))

        self.image_count += 1

    def refresh(self):
        self.driver.refresh()


def main():
    parser = argparse.ArgumentParser(description='captcha download')
    parser.add_argument('--images-amount', type=int, default=1_000, metavar='N',
                        help='amount captcha images to download (default: 1000)')
    parser.add_argument('--data-path', help='paste path to biog.txt file')

    args = parser.parse_args()

    bot = CaptchaBot(args.data_path)
    for _ in tqdm(range(args.images_amount)):
        bot.download_captcha()
        bot.refresh()


if __name__ == '__main__':
    main()


"""
python captcha_dataset_collection.py --images-amount=1000 --data-path="../data/"
"""