# SSB_API_helper
Writing the "query" statement to make an API call to StatBank of Statistics Norway (SSB) can be quite annoying. The functions here help forming the query string by pre-populating the query with available variables.

## Usage

To get table [09189](https://www.ssb.no/en/statbank/table/09189), one need to call

```python
df_gdp = ssb_get_table('09189')
```

This acquires the entire table and returns it as a pandas dataframe. One can alternately call

```python
available_vars = ssb_get_var_info('09189')
```
to get a dict of available variables, and then feed in updated `available_var` to `ssb_get_table` to get a subset of the available variables.

```python
available_vars = ssb_get_var_info('09189')
# After editing available_vars
dg_gdp = ssb_get_table('09189', vars_cell = available_vars)
```

One can also download the table in separate chunks and put a wait time between API call by setting `n_step` and `wait_time`.

Lastly, though this is a thin wrapper around pandas.pivot function, I frequently rotate the result dataframe with

```python
df_gdp = ssb_rotate_table(df_gdp)
```
to get wide table with years (`ind`) as indices.

## Todo:

* Separate example files
