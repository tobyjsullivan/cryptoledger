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

	if result.exchange != expected.exchange {
		t.Error("Unexpected exchange:", result.exchange, "Expected:", expected.exchange)
	}

	if result.baseCurrency != expected.baseCurrency {
		t.Error("Unexpected base currency:", result.baseCurrency, "Expected:", expected.baseCurrency)
	}

	if result.quoteCurrency != expected.quoteCurrency {
		t.Error("Unexpected quote currency:", result.quoteCurrency, "Expected:", expected.quoteCurrency)
	}

	if result.txnType != expected.txnType {
		t.Error("Unexpected txnType:", result.txnType, "Expected:", expected.txnType)
	}

	if result.timestamp != expected.timestamp {
		t.Error("Unexpected timestamp:", result.timestamp, "Expected:", expected.timestamp)
	}

	if result.amount != expected.amount {
		t.Error("Unexpected amount:", result.amount, "Expected:", expected.amount)
	}

	if result.price != expected.price {
		t.Error("Unexpected price:", result.price, "Expected:", expected.price)
	}

	if result.fee != expected.fee {
		t.Error("Unexpected fee:", result.fee, "Expected:", expected.fee)
	}
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
