def load_file(filename):  # define a function to load your data, one input parameter is the name of the file to be loaded
  import pandas as pd
  data = pd.read_csv(filename) # load file with given name inside the function
  return data  # tell function to return the loaded data
