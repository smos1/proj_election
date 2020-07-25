import cv2
import numpy as np
import pandas as pd
import os
import torch
import torchvision.transforms as transforms

DATA_PATH = '../data'
DIGIT_WIDTH = 12
DIGIT_HEIGHT = 12

# digit dataset mean and std
normalize_transform = transforms.Normalize((0.1570,), (0.3211,))


def load_labels():
    labels = pd.read_csv(
        os.path.join(DATA_PATH, 'annotations.csv'),
        header=None,
        sep=';',
        usecols=[0, 1],
        names=['files', 'label', '_'],
        dtype={'label': 'str'}
    )
    labels['label'] = labels['label'].apply(lambda x: str(x))
    labels.set_index('files', inplace=True)
    labels = labels.to_dict('index')

    return labels


def image_resize(image, width=None, height=None, inter=cv2.INTER_AREA):
    """
    image resizeing without distortion
    """
    image = image[:height, :width]
    (h, w) = image.shape[:2]

    # padding
    result = np.zeros((height, width), dtype=image.dtype)

    u = (height - h) // 2
    l = (width - w) // 2

    result[u:u + h, l:l + w] = image

    # return the resized image
    return result


def preprocess_digits(digits):
    digits = [torch.Tensor(image_resize(digit, DIGIT_WIDTH, DIGIT_HEIGHT)) for digit in digits]
    digits = [normalize_transform(x.unsqueeze(0) / x.max()) for x in digits]
    digits = torch.cat([x.unsqueeze(0) for x in digits], 0)
    return digits
