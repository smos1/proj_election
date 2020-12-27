from captcha_solver.solver import solve_captcha
from flask import Flask, render_template,request
import torch
import  os
import sys
import cv2
import numpy as np
import re


app = Flask(__name__)

script_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(script_path)
model_path = os.path.join(script_path, 'captcha_solver/models/digit_cnn.pt')
DEVICE = torch.device('cpu')


@app.route("/", methods = ["GET","POST"])
def predict():
    cnn_model.eval()
    if request.method == "POST":
        image_file = request.files["image"]
        if image_file:
            array_image = np.fromfile(image_file, np.uint8)
            image = cv2.imdecode(array_image, cv2.IMREAD_COLOR)
            data = np.asarray(image)
            pred = solve_captcha(data)
            return render_template("index.html", prediction=pred, image_loc=image_file.filename)
    return render_template("index.html", prediction=0, image_loc=None)


@app.route("/")
def index():
    return render_template("index.html")

if __name__ == "__main__":
    cnn_model = torch.load(model_path, map_location=DEVICE)
    cnn_model.to(DEVICE)
    app.run(host="0.0.0.0", port=12000, debug=True)
