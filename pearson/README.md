pearson.cpp looks at minute bars over 2022 and outputs pearson.csv

pearson_historical.py can both pull historical hourly bars, truncate and sort the result, or simply truncate and sort a database which already has historical bars.
Call python3 pearson_historical.py with the following options:
-r or --refresh=: whether to pull historical hourly bars and perform a Pearson correlation based on them. If 1 or True, we need a file named pearson.csv which doesn't already have a 'pearson_historical' column. If 0 or False, we need a file named pearson_historical.csv with a 'pearson_historical' column.
-c or --cutoff=: the cutoff value in absolute value for the last year (the 'pearson' column). Defaults to 0.9.
-t or --historical-cutoff=: the cutoff value in absolute value for the 'pearson_historical' column.
-s or --sparse_cutoff=: the minimum number of minute bars over the trailing year the two symbols need to have in common to not be thrown out. Defaults to 15000. (The reason is that symbols with few bars in common are harder to regress and can't be trusted.)

The files pearson_historical_9x_9x.csv consist of those symbols with pearson cutoffs = 0.9x and with sparse pairs discarded.