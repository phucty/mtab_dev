"""
Load FastAPI instance and MTab functions
"""
from fastapi import FastAPI

from api import m_f
from api_f.f_config import *

# Init FastAPI instance
app = FastAPI(**api_funcs_info)

# Init MTab services
m_f.init(is_log=False)

# Import MTab functions to FastAPI
from api_f import f_funcs
