package main

import (
	"time"
	"encoding/csv"
	"os"
	"log"
	"fmt"
	"strings"
)

const (
	TransactionTypeTradeBuy = txnType("buy") // Buy the base currency
	TransactionTypeTradeSell = txnType("sell") // Sell the base currency
	TransactionTypeWithdrawal = txnType("withdrawal")
	TransactionTypeDeposit = txnType("deposit")

	CurrencyBTC = currency("BTC")
	CurrencyETH = currency("ETH")
	CurrencyLTC = currency("LTC")
	CurrencyCAD = currency("CAD")
	CurrencyUSD = currency("USD")

	ExchangeGdax = "gdax"
	ExchangeCoinbase = "coinbase"
	ExchangeQuadriga = "quadriga"
)

var (
	UnitDivisors = map[currency]int {
		CurrencyBTC: 100000000,
		CurrencyETH: 100000000,
		CurrencyLTC: 100000000,
		CurrencyCAD: 100,
		CurrencyUSD: 100,
	}

	csvHeaders = []string{
		"exchange",
		"base_currency",
		"quote_currency",
		"transaction_type",
		"timestamp",
		"amount",
		"price",
		"fee",
	}

	validExchanges = []string{
		ExchangeGdax,
		ExchangeCoinbase,
		ExchangeQuadriga,
	}

	exchangeReaders = map[string]inputReader {
		ExchangeGdax: readGdax,
	}
)

/*
 * This tool converts a CSV transaction record from an exchange to a normalized format
 */
func main() {
	if len(os.Args) != 4 {
		println("error: invalid usage")
		println(usage())
		os.Exit(1)
	}

	exchange := os.Args[1]
	validExchange := false
	for candidate := range exchangeReaders {
		if candidate == exchange {
			validExchange = true
			break
		}
	}
	if !validExchange {
		println("error: invalid exchange:", exchange)
		println(usage())
		os.Exit(1)
	}

	baseCurrency := os.Args[2]
	quoteCurrency := os.Args[3]

	fin := os.Stdin
	fout := os.Stdout

	meta := &orderBook{
		baseCurrency: currency(baseCurrency),
		quoteCurrency: currency(quoteCurrency),
	}

	reader, found := exchangeReaders[exchange]
	if !found {
		log.Fatalln("[main] no reader found for exchange:", exchange)
	}

	txns, err := reader(fin, meta)
	if err != nil {
		log.Fatalln("[main] failed to read full input file:", err)
	}

	err = writeTransactions(fout, txns)
	if err != nil {
		log.Fatalln("[main] error writing transactions:", err)
	}

	log.Println("[main] Completed successfully.")
}

func usage() string {
	prog := os.Args[0]

	exchanges := make([]string, 0)
	for exchange := range exchangeReaders {
		exchanges = append(exchanges, exchange)
	}
	exchangeList := strings.Join(exchanges, "|")

	return fmt.Sprintf("usage: %s <format: %s> <basecurrency> <quotecurrency>", prog, exchangeList)
}

type orderBook struct {
	baseCurrency currency
	quoteCurrency currency
}

type record struct {
	exchange string
	baseCurrency currency
	quoteCurrency currency
	txnType txnType
	timestamp time.Time
	amount string
	price string
	fee string
}

func (r *record) CSV() []string {
	return []string{
		r.exchange,
		string(r.baseCurrency),
		string(r.quoteCurrency),
		string(r.txnType),
		r.timestamp.UTC().Format(time.RFC3339),
		r.amount,
		r.price,
		r.fee,
	}
}

type currency string

type txnType string

func writeTransactions(f *os.File, txns chan *record) error {
	w := csv.NewWriter(f)
	err := w.Write(csvHeaders)
	if err != nil {
		return err
	}
	for txn := range txns {
		err = w.Write(txn.CSV())
		if err != nil {
			return err
		}
	}
	w.Flush()
	err = f.Close()

	return err
}

type inputReader func(*os.File, *orderBook) (chan *record, error)

func readGdax(file *os.File, book *orderBook) (chan *record, error) {
	r := csv.NewReader(file)
	entries, err := r.ReadAll()
	if err != nil {
		return nil, err
	}

	startRow := 1

	txns := make(chan *record)

	go func(txns chan *record) {
		for i, entry := range entries {
			if i < startRow {
				continue
			}

			rec, err := ParseGdaxRecord(book, entry)
			if err != nil {
				log.Println("[readGdax] Error parsing record: ", err ,". line:", i)
			}

			txns <- rec
		}

		close(txns)
	}(txns)

	return txns, nil
}

func ParseGdaxRecord(book *orderBook, values []string) (*record, error) {
	colSide := 2
	colTimestamp := 3
	colSize := 4
	colPrice := 6
	colFee := 7

	timeFmt := "2006-01-02T15:04:05.999Z07:00"

	var err error
	var txnType txnType
	var timestamp time.Time
	var amount string
	var price string
	var fee string

	switch values[colSide] {
	case "BUY":
		txnType = TransactionTypeTradeBuy
	case "SELL":
		txnType = TransactionTypeTradeSell
	}

	timestamp, err = time.Parse(timeFmt, values[colTimestamp])
	if err != nil {
		return nil, err
	}

	amount = values[colSize]
	price = values[colPrice]
	fee = values[colFee]

	return &record{
		exchange: ExchangeGdax,
		baseCurrency: book.baseCurrency,
		quoteCurrency: book.quoteCurrency,
		txnType: txnType,
		timestamp: timestamp,
		amount: amount,
		price: price,
		fee: fee,
	}, nil
}
