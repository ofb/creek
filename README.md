# creek
## Strategy
1. Obtain historical price data for anything tradable (I will simplify below and assume stocks)
2. Linearly regress pairs of stocks against each other, or one against several, until you find a correlated combination (e.g. an attractive R^2)
3. For this combination (to simplify I assume pair), compute a probabilistic linear regression to produce a probability distribution with mean µ(x)
4. Using this model, given live prices of the regressor (x) and regressand (y) as inputs, one computes the distance |y – µ(x)| as a percentage % of the standard deviation of your probability distribution at that x
5. Your ‘signal’ is when this percentage % exceeds some threshold, at which point you short y and long x (if y > µ(x)) or long y and short x (if y < µ(x))
6. Integrate this recipe over time; i.e. your position is a linear function of % so in particular as % -> 0, you pare the trade to zero, or more simply, you maintain your position until the signal disappears or reverses.

## Pipeline
### Historical collection
The script historical_data.py downloads historical data bars from alpaca and saves them as .csv files in /mnt/disks/creek-1/. It saves one file per symbol. The list of symbols to collect and the list of processed symbols must be present as .csv files in the same directory as historical_data.py. The current list of collectible symbols comes from alpaca’s asset list, filtered to only retain tradable, shortable us equities. The necessity of this is clear, as pair trading strategies require shorting. We do not require, however, that the symbols be fractionable, as too few are available for fractional trading on alpaca.

historical_data.py -b <number_of_symbols> -i <interval> -y <years>

The above is the calling format for historical_data.py. Interval can be Minute, Hour, or Day. Alpaca doesn’t currently carry bars from before 2015. The todo and processed lists depend on the interval and take the format symbol_Interval_todo.csv and symbol_Interval_processed.csv (e.g. symbol_Minute_todo.csv). historical_data.py prompts for the directory where to save the bars. Currently on /mnt/disks/creek-1/ we have the following directories for historical collection:
- us_equities: minute bars going back as far as available
- us_equities_hourly: hour bars going back as far as available
- us_equities_2022: minute bars from 1/1/2022 to about 8/6/2022

#### Hardware requirements
Can run on low-powered VMs, such as e2-small. However, runs too slow to be useful on e2-micro. On e2-small, 300 minute bars going back to 2015 takes a whole day, and each full bar is about 40 MB.

### Analysis
#### Outline
1. We prepare our 2022 minute bars by interpolating every minute linearly, then truncating, so that every symbol has the same number of bars, saving in /mnt/disks/creek-1/us_equities_2022_interpolated.
2. We compute pearson correlations for every pair that appears on shortable_equity_list.csv based on 2022 minute bars using pearson.cpp.
3. For those pairs with abs(pearson) > 0.9 (about 200,000 pairs), we compute pearson correlations based on historical (2015–) hour bars using pearson_historical.py.
4. We compute probabilistic linear regressions for every pair meeting a certain pearson threshold using tf.py. This is computationally expensive, so the trained model weights (sufficient to reconstitute the model) are saved in a checkpoints directory.


#### interpolate.py
Two pairs may have some minute bars in common but not others. As it’s easier to find the minutes they have in common using the formalism of pandas dataframes, we preprocess the 2022 minute bars so that by the end all symbols have the same number of bars at the same set of indices. This interpolation step is not computationally expensive.

#### pearson.cpp
This C++ program computes the pearson correlation between every pair of symbols on shortable_equity_list.csv. When this list contains ~5,300 symbols, there are 14 million pairs. Therefore this correlation process is computationally intensive. It is parallelized and took about 3.25 hours on a four-core computational VM (27 minutes of that was to load the databases), but the parallelization of the for loop using OpenMP is not optimal as it appears to break the iterations of the loop into four contiguous blocks, while the later cases are faster to compute than the earlier ones. Therefore, it would be more efficient to farm out interleaved iterations. pearson.cpp outputs to pearson.csv.

pearson_historical.py
pearson_historical.py -r <refresh> -c <last_year_cutoff> -t <historical_cutoff> -s <sparse_cutoff>
pearson_historical.py first truncates the pairs on pearson.csv to discard those with | pearson | < 0.9. For the pairs that remain, it computes historical pearson correlations based on historical hour bars (/mnt/disks/creek-1/us_equities_hourly) and adds these historical pearson correlations as a new column for these pairs. pearson_historical.py also applies some sorting and filtering of results. We can choose where to cut off the 2022 minute pearson correlations and the historical hourly pearson correlations. The default is to cut off both at 0.9, but lists with both at 0.95, 0.94, 0.93, 0.92, and 0.91 have been made. The results are sorted by historical correlation.

If <refresh> is set to 1, pearson_historical.py takes as its data source pearson.csv and computes historical pearson correlations.
If <refresh> is set to 0, pearson_historical.py takes as its data source pearson_historical.csv and skips computing historical pearson correlations. This is useful when all you want to do is truncate or throw out sparse pairs.
In either case, pearson_historical.py exports its results to pearson_historical_truncated.csv.

Some pairs are sparse, sharing fewer than 15k minute bars over the last year in common. These sparse pairs later create problems for regression convergence, and moreover their correlations often can’t be trusted. Therefore if the parameter <sparse_cutoff> is set to an integer > 0, pearson_historical.py will pull minute bars from the last year for all the symbols in the file pearson_historical. If <sparse_cutoff> = 0, pearson_historical.py will not check for sparse bars and will not pull minute bars.

Another issue for regression convergence is if the first symbol in a pair is much larger than the second. Therefore, as part of the sparse_cutoff routine, pearson_historical.py swaps the symbol order so that the symbol with the larger average price (using the last year’s minute bars) appears second. Note that this swapping will not occur if <sparse_cutoff> = 0. (This is because the sparse_cutoff routine is the only part of pearson_historical.py that needs to load the last year’s minute bars.)

#### Performance considerations
Truncating (<refresh> = 0, <sparse_cutoff> = 0) has very modest hardware requirements and can probably even be run on e2-micro.
Refreshing (<refresh> = 1) is slow on e2-small and should be run on e2-medium or above.
Sparse truncating (<sparse_cutoff> > 0) requires pulling minute bars for the trailing year, which are somewhat heavy (~1.2 GB / 1000 symbols), so should be run on a system with enough memory.

pearson_historical_9x_9x.csv
These files contain pairs with pearson correlates ≥ 0.9x with sparse pairs (< 15,000 minute bars in common) thrown out. The versions including sparse bars can be found in /mnt/disks/creek-1/pearson/.
- pearson_historical_90_90.csv has 11,811 pairs
- pearson_historical_91_91.csv has 7,686 pairs
- pearson_historical_92_92.csv has 4,578 pairs
- pearson_historical_93_93.csv has 2,454 pairs
- pearson_historical_94_94.csv has 1,148 pairs
- pearson_historical_95_95.csv has 459 pairs

#### tf.py
tf.py uses the tensorflow probability library to build probabilistic linear regressions for every pair on the pearson list. It runs off the last year’s minute bars.

tf.py -r <refresh> -e <epochs> -m <missing>
If refresh is 0, tf.py looks in the checkpoints directory and doesn’t retrain a model for any pair that already appears there. Default is 1.
Epochs is number of epochs, default is 100. Early stopping means fewer than 100 epochs may be run.
The -m flag takes 0 or 1. The default is 0, which doesn’t affect program behavior. If --missing=1, the program ignores its usual behavior and simply opens pearson.csv makes a list of all pairs that don’t appear under the dev folder which is a parameter set at the beginning of the file (e.g. /mnt/disks/creek-1/tf/dev), and saves this list as missing_pairs.csv. 

#### Issues affecting convergence
- Sparseness: pairs of symbols with fewer than 15,000 bars in common regress poorly and sometimes don’t converge at all. Therefore pearson_historical.py has a <sparse_cutoff> parameter to throw away those pairs with few bars in common.
- Imbalanced prices: convergence works best when the prices of the two symbols are of the same order of magnitude. Problems arise when one is much larger than the other. It is important, in such a situation, that the larger symbol be the regressand (dependent/y variable) and not the regressor (independent/x variable). When the larger of the two is the regressand, convergence is slow but is achieved. When the larger of the two is the regressor, convergence may simply be achieved. This was observed with the symbol NVR in particular, since one share of NVR in 2022 costs > $5,000. NVR-PHM never converged, while PHM-NVR converged after about 40 epochs. To address this issue, pearson_historical.py reorders pairs so that the symbol with the larger average price is the second.

The probabilistic linear regression for a pair generally converges at 25 epochs, but not always if the seed is unlucky. Almost all converge at 75 epochs except for problematic sparse ones. However, there are some well-correlated, non-sparse pairs that don’t quite converge at 75 epochs. tf.py implements early stopping: if a regression doesn’t improve by 0.001 in 10 epochs, the regression terminates and the best epoch is selected (technically the selected epoch is guaranteed to have loss < 0.001 more than the strictly best one).

It usually takes about 1–2 minutes to fit the tensorflow model to a given pair at 25 epochs, and 3–5 minutes at 75 epochs. Therefore the computation of the 631 pairs with correlation > 0.95/0.95 would take about 10 at 25 epochs single-threaded and 30 hours at 75 epochs. Therefore, computation has been parallelized and it’s best to use the 60-core computational GCP VM. On that VM the 25 epochs the 631-pair computation took about 10 minutes, and half an hour at 75. (An hour costs about $3.) As there are about 1,600 0.94/0.94 pairs before throwing out the sparse ones, a fresh tensorflow regression computation on those pairs should take about 75 minutes (~$4).

For each pair, tf.py saves a checkpoint file (enough to reconstruct the model with no re-training), a .csv that records the percentage of one standard deviation for each minute that the pairs have in common, an image of the dataset with mean and ±2 stddev lines, and a loss history graph.

There is a further processing file, tf_summary.py, which loads all the pairs and their deviations over the course of the year, and finds how many pairs are outside of 2 sigma at every given minute. In 2022, looking at the 631 0.95/0.95 pearson pairs, after a wild first half with between 10 and 50 at any given moment outside of 2 sigma, things have quieted down to only about 2–5 at any given moment outside of 2 sigma. Therefore, as discussed below, we will need to dip into the > 1,600 0.94/0.94 pairs to obtain sufficient diversification.

From the …_dev.csv files for each pair, one can easily make charts plotting the deviation as a percentage of the standard deviation. For one example AIRC/AVB, this number never stays above 2 for very long, and often returns to below 0.5. This informs our signals discussed in the next section.
