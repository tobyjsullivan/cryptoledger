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
cat <input-csv> | normalize gdax ETH BTC > output.csv
```

In this example:
* `gdax` is the exchange format of the input csv.
* `ETH` is the base currency of the current book (e.g., "ETH/BTC").
* `BTC` is the quote currency of the current book (e.g., "ETH/BTC").
* The input csv is read from `stdin`
* The normalized csv is written to `stdout`
