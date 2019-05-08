from pyjstat import pyjstat
import requests
import json
import pandas as pd
import time

def ssb_parse_url(table_num):
  """Function that creates the URL to SSB api (internal function)

  Args:
    table_num (str): table number of StatBank to acquire

  Returns:
    str: URL address of the table
  """

  return 'http://data.ssb.no/api/v0/en/table/' + str(table_num)


def ssb_get_var_info(table_num):
  """Get available variables from the table

  Args:
    table_num (str): table number of the Statbank to inquire

  Returns:
    cell: {variable_code: list of vars} to be used <ssb_parse_query> function
  """

  POST_URL = ssb_parse_url(table_num)

  metadata = requests.get(url=POST_URL)
  metadata = json.loads(metadata.text)
  variables = metadata['variables']
  return {var["code"]:var["values"] for var in variables}


def ssb_parse_query(variables):
  """Parses query to send to SSB api

  Args:
    variables (cell): {variable_code: list of vars} to get from the table

  Returns:
    str: query to send to SSB api

  Note:
    One should edit the results from <ssb_get_var_info> function that
    populates what variable codes and variables are available from the table
  """

  payload = { "query": [{ "code": var_key,
                          "selection": {"filter":"item",
                                        "values": var_val}}
                              for var_key, var_val in variables.items()],
              "response": {
                "format": "json-stat2"
              }
            }
  return payload


def ssb_get_table(table_num, n_step = 1, vars_cell = 1, wait_time = 10):
  """Get data from SSB api

  Args:
    table_num (str): table number of the Statbank to inquire
    n_step (int): (default: 1) Number of separate calls to make to SSB
    vars_cell (cell): {variable_code:list of vars} to get; if not specified
      get all variables
    wait_time (float): (default: 5 sec) wait time in seconds between calls

  Return:
    pandas.dataframe: dataframe of the table

  Note:
    Statbank puts a limit on the number of cells one can download in one API
    call. Hence, increase n_step if the API return error code 403.

  See also:
    - rotate_table
  """

  if not isinstance(vars_cell, dict):
    vars_cell = ssb_get_var_info(table_num)

  print('Block 1 of ' + str(n_step))

  POST_URL = ssb_parse_url(table_num)
  payload = ssb_parse_query(vars_cell)

  ### Ugly patch by looping that works for now
  block = len(vars_cell[payload["query"][-1]["code"]])//n_step
  df = []
  for iter in range(n_step-1):
    payload["query"][-1]["selection"]["values"] = vars_cell[payload["query"][-1]["code"]][block*iter:(iter+1)*block]
    start_time = time.time()

    response = requests.post(POST_URL, json = payload, timeout=5)
    print(response)

    dataset = pyjstat.Dataset.read(response.text)
    df = df + [dataset.write('dataframe')]

    process_time = time.time() - start_time
    if process_time < wait_time:
      print("Pause before making the next call")
      time.sleep(wait_time-process_time)
    print('Block ' + str(iter+2) + ' of ' + str(n_step))

  payload["query"][-1]["selection"]["values"] = vars_cell[payload["query"][-1]["code"]][(n_step-1)*block:]

  response = requests.post(POST_URL, json = payload, timeout=5)
  print(response)

  dataset = pyjstat.Dataset.read(response.text)
  df = df + [dataset.write('dataframe')]
  df = pd.concat(df)
  ###

  return df


def ssb_rotate_table(df, ind='year', val='value'):
  """Rotate the dataframe so that years are used as the index

  Args:
    df (pandas.dataframe): dataframe (from <get_from_ssb> function
    ind (str): string of column name denoting time
    ind (str): string of column name denoting values

  Returns:
    dataframe: pivotted dataframe
  """

  return df.pivot_table(index=ind,
                        values=val,
                        columns=[iter for iter in df.columns \
                                      if iter != ind and iter != val])
