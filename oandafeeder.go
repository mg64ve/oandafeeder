package main

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"

	"github.com/davecgh/go-spew/spew"
	goa "github.com/mg64ve/goanda"
)

type ByTime []goa.Candles

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
        // User/Toker for Oanda API
	User string `json:"user"`
	Token string `json:"token"`
}

// OandaFetcher is the main worker instance.  It implements bgworker.Run().
type OandaFetcher struct {
	config        map[string]interface{}
	symbols       []string
	queryStart    time.Time
	baseTimeframe *utils.Timeframe
        oandaUser     string
        oandaToken    string
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
        oandaUser := config.User
        oandaToken := config.Token
	return &OandaFetcher{
		config:        conf,
		symbols:       symbols,
		queryStart:    queryStart,
		baseTimeframe: utils.NewTimeframe(timeframeStr),
                oandaUser:     oandaUser,
                oandaToken:    oandaToken,
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
	var rates []goa.Candles
	var endTime, timeEnd, timeStart, lastTime time.Time

	symbols := oa.symbols
	numBars := time.Duration(4999)

	key := oa.oandaToken
	accountID := oa.oandaUser

	client := goa.NewConnection(accountID, key, false)

	timeStart = time.Time{}
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
		timeEnd = timeStart.Add(oa.baseTimeframe.Duration * numBars)
		lastTime = timeStart
		endTime = time.Now()
		for _, symbol := range symbols {
			granularity := "D"
			switch oa.baseTimeframe.Duration.Seconds() {
			case 60:
				granularity = "M1"
			case 300:
				granularity = "M5"
			case 3600:
				granularity = "H1"
			default:
				granularity = "D"
			}

			fmt.Printf("Requesting %s %v - %v (Using granularity: %s)\n", symbol, timeStart, timeEnd, granularity)
			ts := timeStart.Unix()
			te := timeEnd.Unix()
			time_s := strconv.FormatInt(ts, 10)
			time_e := strconv.FormatInt(te, 10)
			fmt.Printf("Calling Oanda for %s, from: %s, to: %s, granularity: %s\n", symbol, time_s, time_e, granularity)
			his := client.GetCandlesFromTime(symbol, time_s, time_e, granularity)
			rates = his.Candles
			//spew.Dump(rates)
			if len(rates) == 0 {
				fmt.Printf("len(rates) == 0\n")
				time.Sleep(time.Minute)
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
			endTime = rates[(len(rates))-1].Time
			fmt.Printf("%s: %d rates between %v - %v\n", symbol, len(rates),
				rates[0].Time, rates[(len(rates))-1].Time)
			csm := io.NewColumnSeriesMap()
			tbk := io.NewTimeBucketKey(symbol + "/" + oa.baseTimeframe.String + "/OHLCV")
			csm.AddColumnSeries(*tbk, cs)
			executor.WriteCSM(csm, true)
		}
		// next fetch start point
		timeStart = endTime
		// for the next bar to complete, add it once more
		nextExpected := timeStart.Add(oa.baseTimeframe.Duration * numBars)
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


	key := "YOUR_OANDA_TOKEN"
	accountID := "YOUR_OANDA_USER"
	symbol := "EUR_USD"

	client := goa.NewConnection(accountID, key, false)

	var rates []goa.Candles
	numBars := time.Duration(4999)
	granularity := "M1"
	timeStart := time.Date(2017, 12, 1, 0, 0, 0, 0, time.UTC)
	timeEnd := timeStart.Add(time.Minute * numBars)
	ts := timeStart.Unix()
	te := timeEnd.Unix()
	time_s := strconv.FormatInt(ts, 10)
	time_e := strconv.FormatInt(te, 10)
	fmt.Printf("Requesting %s %v - %v (Using granularity: %s)\n", symbol, timeStart, timeEnd, granularity)
	history := client.GetCandlesFromTime(symbol, time_s, time_e, granularity)
	rates = history.Candles
        r := rates[0]
	fmt.Printf("r: %v, %v\n", r.Time, r.Mid.Low)
	spew.Dump(rates)
	fmt.Printf("len(rates) == %d\n", len(rates))

	// test loop
	for i := 0; i < 20; i++ {
		timeStart = rates[(len(rates))-1].Time
		timeEnd := timeStart.Add(time.Minute * numBars)
		fmt.Printf("%s: between %v - %v\n", symbol, timeStart, timeEnd)
		ts := timeStart.Unix()
		te := timeEnd.Unix()
		time_s := strconv.FormatInt(ts, 10)
		time_e := strconv.FormatInt(te, 10)
		fmt.Printf("Requesting %s %v - %v (Using granularity: %s)\n", symbol, timeStart, timeEnd, granularity)
		history := client.GetCandlesFromTime(symbol, time_s, time_e, granularity)
		rates = history.Candles
		//spew.Dump(rates)
		fmt.Printf("len(rates) == %d\n", len(rates))
	}

}
