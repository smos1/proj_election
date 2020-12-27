import cv2
import glob
import numpy as np
import torch
import os
import sys

script_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(script_path)

from model import Net
from extract_single_digits import get_single_digits
from utils import load_labels, DATA_PATH, preprocess_digits

model_path = os.path.join(script_path, 'models/digit_cnn.pt')
device = torch.device('cpu')

cnn_model = torch.load(model_path, map_location=device)
cnn_model.eval()


def solve_captcha(image):
    digits = get_single_digits(image)
    digits = preprocess_digits(digits)

    with torch.no_grad():
        results = torch.argmax(cnn_model(digits), 1)

    return ''.join([str(x) for x in results.tolist()])


def captcha_solver_validation(verbose=False):
    cnn_model = torch.load(model_path, map_location=device)
    labels = load_labels()
    valid_path = os.path.join(DATA_PATH, 'annotated_data/valid')

    captcha_correct, digit_correct = [], []
    for i, image_path in enumerate(glob.glob(os.path.join(valid_path, "*.png"))):
        image = cv2.imread(image_path)

        file = image_path.split(os.path.sep)[-1]
        result = solve_captcha(image)

        true_label = labels.get(file, {'label': None})['label']

        captcha_correct.append(true_label == result)
        digit_correct += [x == y for x, y in zip(true_label, result)]

        if true_label != result and verbose:
            print(f'file: {file}, label: {true_label}, predicted: {result}')

    return np.array(captcha_correct).mean(), np.array(digit_correct).mean()
