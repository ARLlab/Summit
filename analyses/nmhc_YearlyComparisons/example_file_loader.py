def load_file():  # define a function to load your data
  import pandas as pd
  data = pd.read_csv('my_fake_file.csv') # load file within the function
  return data  # tell function to return the loaded data
