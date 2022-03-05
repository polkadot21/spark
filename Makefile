covid:
	spark-submit load_covid_df.py data/COVID_19.csv
author:
	spark-submit create_dataFrame.py
all:
	covid, author