# CryptoLedger

Compute capital gains for crypto trades.

## Normalize

The normalize tool transforms exchange-specific csv exports to a normalized format.

### Install

```
go install ./normalize
```

### Usage

```
normalize gdax ETH BTC <input-csv> output.csv
```

In this example:
* `gdax` is the exchange format of the input csv.
* `ETH` is the base currency of the current book (e.g., "ETH/BTC").
* `BTC` is the quote currency of the current book (e.g., "ETH/BTC").
* `<input-csv>` should be replaced with the path to the csv you are transforming.
* `output.csv` is the output file to write to. Any existing file will be overwritten.
