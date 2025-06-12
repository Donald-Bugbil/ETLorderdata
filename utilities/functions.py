import pandas as pd
import re

#cleaning custoer_name column
def name_converter(x):
    try:
        full_name=" "
        name_split=x.split()
        for real_name in name_split:
            cap=real_name.capitalize()
            full_name=full_name+ " " +cap
            return full_name
    except AttributeError:
        return x
    
#cleaning country name format
def country_name(x):
    x=x.strip()
    if x in ['us','United States','U.S.A.']:
        return 'USA'
    return x

#cleaning state column 
def state_name(state):
    state=state.strip( )
    if state in ['CA','CALIFORNIA']:
        return 'California'
    return state

#validating email
def invalid_email(email):
    email_pattern= re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')
    if pd.isna(email):
        return True
    else:
        return not bool(email_pattern.match(str(email)))
    
#cleaning product_category
def product_category(product):
    if product in ['Sportswear','Sportwear']:
        return 'Sports'
    return product