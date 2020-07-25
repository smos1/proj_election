import glob
import os
import cv2

from utils import DATA_PATH

ANNOTATED_DATA_PATH = os.path.join(DATA_PATH, 'annotated_data/train')


def main():
    for i, image_path in enumerate(glob.glob(os.path.join(DATA_PATH, "*.png"))):
        image = cv2.imread(image_path)

        annotation = []
        for _ in range(5):
            cv2.imshow('image', image)
            annotation.append(chr(cv2.waitKey(0)))

        annotation = ''.join(annotation)

        if annotation.isdigit():
            with open(os.path.join(DATA_PATH, 'annotations.csv'), 'a+') as f:
                annotation_str = ';'.join([image_path.split(os.path.sep)[-1], annotation, '\n'])
                print(annotation_str)
                f.write(annotation_str)
            os.system(f'mv {image_path} {ANNOTATED_DATA_PATH}')

        elif annotation == 'delet':
            os.system(f'rm -f {image_path}')


if __name__ == '__main__':
    main()
