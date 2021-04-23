# -*- coding: utf-8 -*-
"""
Created on Dec 2019

@edited by: Theo G
"""

# =============================================================================
# code (no need to touch below)
# =============================================================================

from tqdm import tqdm
import pandas as pd
from pathlib import Path
import numpy as np
import sys
import os
from os import chdir, getcwd
wd = getcwd()
chdir(wd)
tqdm.pandas()
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from progressbar import ProgressBar
pbar = ProgressBar()

folder_loc = os.path.dirname(os.path.realpath(__file__))
os.chdir(folder_loc)

# set directory to where this file is located
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
from resources.bot_class import PulsePointBot


if __name__ == "__main__":
    df = PulsePointBot().bot_run()
    df.to_csv('output.csv', index=False)
