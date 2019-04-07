package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"time"
	//"strconv"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"

	goa "github.com/awoldes/goanda"
	//goa "/home/marco/go/src/github.com/awoldes/goanda"
	"github.com/joho/godotenv"
)

type ByTime []goa.Candles

/*
type OInstrumentHistory struct {
     goa.InstrumentHistory
}
type OConnection struct {
     goa.OandaConnection
}


func (c *OConnection) GetCandlesFromTime(instrument string, from string, to string, granularity string) OInstrumentHistory {
        endpoint := "/instruments/" + instrument + "/candles?from=" + from + "&to=" + to + "&granularity=" + granularity
        candles := c.Request(endpoint)
        data := OInstrumentHistory{}
        json.Unmarshal(candles, &data)

        return data
}
*/

func (a ByTime) Len() int           { return len(a) }
func (a ByTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByTime) Less(i, j int) bool { return a[i].Time.Before(a[j].Time) }

// FetchConfig is the configuration for GdaxFetcher you can define in
// marketstore's config file through bgworker extension.
type FetcherConfig struct {
	// list of currency symbols, defults to ["EUR_USD", "AUD_USD", "EUR_GBP", "USD_JPY"]
	Symbols []string `json:"symbols"`
	// time string when to start first time, in "YYYY-MM-DD HH:MM" format
	// if it is restarting, the start is the last written data timestamp
	// otherwise, it starts from an hour ago by default
	QueryStart string `json:"query_start"`
	// such as 5Min, 1D.  defaults to 1Min
	BaseTimeframe string `json:"base_timeframe"`
}

// OandaFetcher is the main worker instance.  It implements bgworker.Run().
type OandaFetcher struct {
	config        map[string]interface{}
	symbols       []string
	queryStart    time.Time
	baseTimeframe *utils.Timeframe
}

func recast(config map[string]interface{}) *FetcherConfig {
	data, _ := json.Marshal(config)
	ret := FetcherConfig{}
	json.Unmarshal(data, &ret)
	return &ret
}

// NewBgWorker returns the new instance of GdaxFetcher.  See FetcherConfig
// for the details of available configurations.
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	symbols := []string{"EUR_USD", "AUD_USD", "EUR_GBP", "USD_JPY"}

	config := recast(conf)
	if len(config.Symbols) > 0 {
		symbols = config.Symbols
	}
	var queryStart time.Time
	if config.QueryStart != "" {
		trials := []string{
			"2006-01-02 03:04:05",
			"2006-01-02T03:04:05",
			"2006-01-02 03:04",
			"2006-01-02T03:04",
			"2006-01-02",
		}
		for _, layout := range trials {
			qs, err := time.Parse(layout, config.QueryStart)
			if err == nil {
				queryStart = qs.In(utils.InstanceConfig.Timezone)
				break
			}
		}
	}
	timeframeStr := "1Min"
	if config.BaseTimeframe != "" {
		timeframeStr = config.BaseTimeframe
	}
	return &OandaFetcher{
		config:        conf,
		symbols:       symbols,
		queryStart:    queryStart,
		baseTimeframe: utils.NewTimeframe(timeframeStr),
	}, nil
}

func findLastTimestamp(symbol string, tbk *io.TimeBucketKey) time.Time {
	cDir := executor.ThisInstance.CatalogDir
	query := planner.NewQuery(cDir)
	query.AddTargetKey(tbk)
	start := time.Unix(0, 0).In(utils.InstanceConfig.Timezone)
	end := time.Unix(math.MaxInt64, 0).In(utils.InstanceConfig.Timezone)
	query.SetRange(start.Unix(), end.Unix())
	query.SetRowLimit(io.LAST, 1)
	parsed, err := query.Parse()
	if err != nil {
		return time.Time{}
	}
	reader, err := executor.NewReader(parsed)
	csm, err := reader.Read()
	cs := csm[*tbk]
	if cs == nil || cs.Len() == 0 {
		return time.Time{}
	}
	ts := cs.GetTime()
	return ts[0]
}

// Run() runs forever to get public historical rate for each configured symbol,
// and writes in marketstore data format.
func (oa *OandaFetcher) Run() {
	symbols := oa.symbols
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	key := os.Getenv("OANDA_API_KEY")
	accountID := os.Getenv("OANDA_ACCOUNT_ID")
	client := goa.NewConnection(accountID, key, false)
	timeStart := time.Time{}
	for _, symbol := range symbols {
		tbk := io.NewTimeBucketKey(symbol + "/" + oa.baseTimeframe.String + "/OHLCV")
		lastTimestamp := findLastTimestamp(symbol, tbk)
		fmt.Printf("lastTimestamp for %s = %v\n", symbol, lastTimestamp)
		if timeStart.IsZero() || (!lastTimestamp.IsZero() && lastTimestamp.Before(timeStart)) {
			timeStart = lastTimestamp
		}
	}
	if timeStart.IsZero() {
		if !oa.queryStart.IsZero() {
			timeStart = oa.queryStart
		} else {
			timeStart = time.Now().UTC().Add(-time.Hour)
		}
	}
	for {
		timeEnd := timeStart.Add(oa.baseTimeframe.Duration * 300)
		lastTime := timeStart
		for _, symbol := range symbols {
			//granularity := strconv.FormatFloat(goa.baseTimeframe.Duration.Seconds(), 'E', -1, 64)
			granularity := "M1"
			fmt.Printf("Requesting %s %v - %v\n", symbol, timeStart, timeEnd)
			numBars := "5000"
			//rates, err := client.GetCandlesFromTime(symbol, timeStart, timeEnd, granularity)
			his := client.GetCandles(symbol, numBars, granularity)
			rates := his.Candles
			/*
				if err != nil {
					fmt.Printf("Response error: %v\n", err)
					// including rate limit case
					time.Sleep(time.Minute)
					continue
				}
			*/
			if len(rates) == 0 {
				fmt.Printf("len(rates) == 0\n")
				continue
			}
			epoch := make([]int64, 0)
			open := make([]float64, 0)
			high := make([]float64, 0)
			low := make([]float64, 0)
			close := make([]float64, 0)
			volume := make([]int, 0)
			sort.Sort(ByTime(rates))
			for _, rate := range rates {
				if rate.Time.After(lastTime) {
					lastTime = rate.Time
				}
				epoch = append(epoch, rate.Time.Unix())
				open = append(open, float64(rate.Mid.Open))
				high = append(high, float64(rate.Mid.High))
				low = append(low, float64(rate.Mid.Low))
				close = append(close, float64(rate.Mid.Close))
				volume = append(volume, rate.Volume)
			}
			cs := io.NewColumnSeries()
			cs.AddColumn("Epoch", epoch)
			cs.AddColumn("Open", open)
			cs.AddColumn("High", high)
			cs.AddColumn("Low", low)
			cs.AddColumn("Close", close)
			cs.AddColumn("Volume", volume)
			fmt.Printf("%s: %d rates between %v - %v\n", symbol, len(rates),
				rates[0].Time, rates[(len(rates))-1].Time)
			csm := io.NewColumnSeriesMap()
			tbk := io.NewTimeBucketKey(symbol + "/" + oa.baseTimeframe.String + "/OHLCV")
			csm.AddColumnSeries(*tbk, cs)
			executor.WriteCSM(csm, false)
		}
		// next fetch start point
		timeStart = lastTime.Add(oa.baseTimeframe.Duration)
		// for the next bar to complete, add it once more
		nextExpected := timeStart.Add(oa.baseTimeframe.Duration)
		now := time.Now()
		toSleep := nextExpected.Sub(now)
		fmt.Printf("next expected(%v) - now(%v) = %v\n", nextExpected, now, toSleep)
		if toSleep > 0 {
			fmt.Printf("Sleep for %v\n", toSleep)
			time.Sleep(toSleep)
		} else if time.Now().Sub(lastTime) < time.Hour {
			// let's not go too fast if the catch up is less than an hour
			time.Sleep(time.Second)
		}
	}
}

func main() {

	//timeStart := time.Date(2017, 12, 1, 0, 0, 0, 0, time.UTC)
	//timeEnd := time.Date(2017, 12, 1, 1, 0, 0, 0, time.UTC)
	granularity := "M1"
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	key := os.Getenv("OANDA_API_KEY")
	accountID := os.Getenv("OANDA_ACCOUNT_ID")
	client := goa.NewConnection(accountID, key, false)
	//res, err := client.GetCandlesFromTime("EUR_USD", timeStart, timeEnd, granularity)
	numBars := "5000"
	res := client.GetCandles("EUR_USD", numBars, granularity)
	fmt.Println(res)
}
