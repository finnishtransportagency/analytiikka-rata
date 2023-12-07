import os
import json

"""

Lukee dev/prod kohtaiset parametrit.

Parametrit pitää olla lambda/glue hakemistossa nimellä <hakemiston nimi>_parameters.json

Sisältö:

{
    "dev": {
        "parametri1": "arvo: dev"
    },
    "prod": {
        "parametri1": "arvo: prod"
    }
}


Arvo pydetään :
path = sama kuin lambda/glue polku
environment = aina muuttuja environment
name = parametrin nimi

val = get_parameter(path = "lambda/testi1", environment = environment, name = "parameter1")

Palautetaan arvo tiedostosta lambda/testi1/testi1_parameters.json


"""
def get_parameter(path: str, environment: str, name: str):
    filename = os.path.join(path, os.path.basename(path) + "_parameters.json")
    value = None
    with open(filename) as json_file:
        data = json.load(json_file)
        if name in data[environment]:
            value = data[environment][name]
            if value == "":
                value = None
    return(value)

