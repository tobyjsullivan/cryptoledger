package main

import (
	"time"
	"encoding/csv"
	"os"
	"log"
	"fmt"
	"strings"
	"strconv"
	"math"
)

const (
	TransactionTypeTradeBuy = txnType("buy") // Buy the base currency
	TransactionTypeTradeSell = txnType("sell") // Sell the base currency

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

	exchangeReaders = map[string]inputReader {
		ExchangeGdax: readGdax,
		ExchangeQuadriga: readQuadriga,
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

func readRecords(file *os.File, book *orderBook, startRow int, parser recordReader) (chan *record, error) {
	r := csv.NewReader(file)
	entries, err := r.ReadAll()
	if err != nil {
		return nil, err
	}

	txns := make(chan *record)

	go func(txns chan *record) {
		for i, entry := range entries {
			if i < startRow {
				continue
			}

			rec, err := parser(book, entry)
			if err != nil {
				log.Println("[readRecords] Error parsing record: ", err ,". line:", i)
			}

			txns <- rec
		}

		close(txns)
	}(txns)

	return txns, nil
}

func readGdax(file *os.File, book *orderBook) (chan *record, error) {
	return readRecords(file, book, 1, ParseGdaxRecord)
}

func readQuadriga(file *os.File, book *orderBook) (chan *record, error) {
	return readRecords(file, book, 1, ParseQuadrigaRecord)
}

type recordReader func(book *orderBook, values []string) (*record, error)

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

func ParseQuadrigaRecord(book *orderBook, values []string) (*record, error) {
	colType := 0
	colTimestamp := 8
	colAmount := 3
	colPrice := 4
	colFee := 6

	var err error
	var txnType txnType
	var timestamp time.Time
	var amount string
	var price string
	var fee string

	switch values[colType] {
	case "buy":
		txnType = TransactionTypeTradeBuy
	case "sell":
		txnType = TransactionTypeTradeSell
	}

	fTime, err := strconv.ParseFloat(values[colTimestamp], 64)
	if err != nil {
		return nil, err
	}
	sec := int64(math.Floor(fTime))
	nsec := int64((fTime - math.Floor(fTime)) * 1000) * int64(time.Millisecond)
	timestamp = time.Unix(sec, nsec).UTC()

	amount = values[colAmount]
	price = values[colPrice]
	fee = values[colFee]

	return &record{
		exchange: ExchangeQuadriga,
		baseCurrency: book.baseCurrency,
		quoteCurrency: book.quoteCurrency,
		txnType: txnType,
		timestamp: timestamp,
		amount: amount,
		price: price,
		fee: fee,
	}, nil
}
