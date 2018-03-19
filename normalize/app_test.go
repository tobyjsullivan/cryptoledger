package main

import (
	"testing"
	"time"
)

func TestParseGdaxRecord(t *testing.T) {
	orderBook := &orderBook{
		baseCurrency: CurrencyETH,
		quoteCurrency: CurrencyBTC,
	}
	input := []string{"398241", "ETH-BTC", "BUY", "2017-04-08T01:22:03.691Z", "1.07003890", "ETH", "0.03727", "0.000119641049409", "-0.039999990852409", "BTC"}
	expected := &record{
		exchange: ExchangeGdax,
		baseCurrency: CurrencyETH,
		quoteCurrency: CurrencyBTC,
		txnType: TransactionTypeTradeBuy,
		timestamp: time.Unix(1491614523, 691000000).UTC(),
		amount: "1.07003890",
		price: "0.03727",
		fee: "0.000119641049409",
	}

	result, err := ParseGdaxRecord(orderBook, input)

	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	validateRecord(t, expected, result)
}

func TestRecord_CSV(t *testing.T) {
	input := &record{
		exchange: ExchangeGdax,
		baseCurrency: CurrencyETH,
		quoteCurrency: CurrencyBTC,
		txnType: TransactionTypeTradeBuy,
		timestamp: time.Unix(1491614523, 691000000).UTC(),
		amount: "1.07003890",
		price: "0.03727",
		fee: "0.000119641049409",
	}
	expected := []string{"gdax", "ETH", "BTC", "buy", "2017-04-08T01:22:03Z", "1.07003890", "0.03727", "0.000119641049409"}

	result := input.CSV()

	for i, resVal := range result {
		if resVal != expected[i] {
			t.Error("Unexpected result:", resVal, "Expected:", expected[i])
		}
	}
}

func TestParseQuadrigaRecord(t *testing.T) {
	orderBook := &orderBook{
		baseCurrency: CurrencyBTC,
		quoteCurrency: CurrencyCAD,
	}
	input := []string{"sell","btc","cad","0.05000000","21499.99","1074.99950000","5.37499750","1069.62450250","1513303913.151","12/15/2017 02:11:53"}
	expected := &record{
		exchange: ExchangeQuadriga,
		baseCurrency: CurrencyBTC,
		quoteCurrency: CurrencyCAD,
		txnType: TransactionTypeTradeSell,
		timestamp: time.Unix(1513303913, 151000000).UTC(),
		amount: "0.05000000",
		price: "21499.99",
		fee: "5.37499750",
	}

	result, err := ParseQuadrigaRecord(orderBook, input)

	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	validateRecord(t, expected, result)
}

func TestParseCoinbaseRecord(t *testing.T) {
	orderBook := &orderBook{
		baseCurrency: CurrencyBTC,
		quoteCurrency: CurrencyCAD,
	}
	input := []string{"2016-02-10 20:06:45 -0800","Buy","0.05452059","28.71","1.29","30.0","CAD","526.59","CIBC ******1234","56bc08d4074f13356d000216","\"\""}
	expected := &record{
		exchange: ExchangeCoinbase,
		baseCurrency: CurrencyBTC,
		quoteCurrency: CurrencyCAD,
		txnType: TransactionTypeTradeBuy,
		timestamp: time.Unix(1455163605, 0).UTC(),
		amount: "0.05452059",
		price: "526.59",
		fee: "1.29",
	}

	result, err := ParseCoinbaseRecord(orderBook, input)

	if err != nil {
		t.Fatal("Unexpected error:", err)
	}

	validateRecord(t, expected, result)
}

func validateRecord(t *testing.T, expected, actual *record) {
	if actual.exchange != expected.exchange {
		t.Error("Unexpected exchange:", actual.exchange, "Expected:", expected.exchange)
	}

	if actual.baseCurrency != expected.baseCurrency {
		t.Error("Unexpected base currency:", actual.baseCurrency, "Expected:", expected.baseCurrency)
	}

	if actual.quoteCurrency != expected.quoteCurrency {
		t.Error("Unexpected quote currency:", actual.quoteCurrency, "Expected:", expected.quoteCurrency)
	}

	if actual.txnType != expected.txnType {
		t.Error("Unexpected txnType:", actual.txnType, "Expected:", expected.txnType)
	}

	if actual.timestamp != expected.timestamp {
		t.Error("Unexpected timestamp:", actual.timestamp, "Expected:", expected.timestamp)
	}

	if actual.amount != expected.amount {
		t.Error("Unexpected amount:", actual.amount, "Expected:", expected.amount)
	}

	if actual.price != expected.price {
		t.Error("Unexpected price:", actual.price, "Expected:", expected.price)
	}

	if actual.fee != expected.fee {
		t.Error("Unexpected fee:", actual.fee, "Expected:", expected.fee)
	}
}
