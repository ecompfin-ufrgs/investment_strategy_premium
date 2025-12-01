# investment_strategy_premium

Trading algorithm based on term premium.

1. Collect daily reference interest-rate term structures from B3 from
2004 to 2024;
2. Apply a cubic spline to the yield curves to obtain a smooth curve;
3. Extract term-premium estimates from the yield curves using the
Adrian, Crump, and Moench (2013) approach;
4. Analyze term-premium dynamics using the Said and Dickey (1984)
and Jarque and Bera (1987) tests;
5. Identify term-premium outliers following (Goldstein; Dengel, 2012);
6. Apply the trading algorithm:
  - Generate trading signals from term-premium outliers using HBOS with rolling windows from 1 to 12 quarters;
  - Classify outliers as:
    - max: unusually high values;
    - min: unusually low values;
  - Apply a mean-reversion trading rule:
    - min → long DI1;
    - max → short DI1.
  - Use the DI1 contract closest to each maturity and handle rollovers;
  - Backtest each maturity separately;
  - Apply a fixed 5% stop-loss and trades only 1 contract;
  - Accumulate daily PnL.
