import cv2
import numpy as np

IMAGE_WIDTH = 130
IMAGE_HEIGHT = 50


def image_preprocess(image):
    # convert to gray
    image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    # scale image sizes
    image = cv2.resize(image, (IMAGE_WIDTH, IMAGE_HEIGHT))
    # invert
    image = image.max() - image
    # add border
    image = cv2.copyMakeBorder(image, 2, 2, 2, 2, cv2.BORDER_CONSTANT)
    return image


def get_single_digits(image):
    image = image_preprocess(image)

    # threshold the image (convert it to pure black and white)
    thresholded = cv2.threshold(image, 0, 255, cv2.THRESH_BINARY)[1]

    # get contours
    contours = cv2.findContours(thresholded, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_NONE)[0]

    # get single digits
    boundings = [cv2.boundingRect(contour) for contour in contours]
    boundings = sorted(boundings, key=lambda x: x[0])
    digits = [image[max(y - 2, 0):y + h + 2, max(x - 2, 0):x + w + 2] for x, y, w, h in boundings]
    return digits
