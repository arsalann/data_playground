"""@bruin
type: python
image: python:3.11
connection: motherduck-prod
@bruin"""

import os
import json


# Get start and end dates from environment variables
start_date = os.environ.get('BRUIN_START_DATE')
end_date = os.environ.get('BRUIN_END_DATE')
print(f"Start date: {start_date}")
print(f"End date: {end_date}")

# Get taxi_type
bruin_vars = json.loads(os.environ["BRUIN_VARS"])
taxi_types = bruin_vars.get('taxi_types')
print(f"Taxi types: {taxi_types}")