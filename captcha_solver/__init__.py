import os
import sys

script_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(script_path)

from solver import solve_captcha
