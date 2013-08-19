package outlet

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"l2met/bucket"
	"l2met/utils"
	"net/http"
	"strconv"
	"time"
)

var libratoUrl = "https://metrics-api.librato.com/v1/metrics"

type LibratoAttributes struct {
	Min   int    `json:"display_min"`
	Units string `json:"display_units_long"`
}

type LibratoPayload struct {
	Name   string             `json:"name"`
	Time   int64              `json:"measure_time"`
	Val    string             `json:"value,omitempty"`
	Source string             `json:"source,omitempty"`
	User   string             `json:",omitempty"`
	Pass   string             `json:",omitempty"`
	Attr   *LibratoAttributes `json:"attributes,omitempty"`
	Count  *int               `json:"count,omitempty"`
	Sum    *float64           `json:"sum,omitempty"`
	Min    *float64           `json:"min,omitempty"`
	Max    *float64           `json:"max,omitempty"`
}

type LibratoRequest struct {
	Gauges []*LibratoPayload `json:"gauges"`
}

type LibratoOutlet struct {
	// The inbox is used to hold empty buckets that are
	// waiting to be processed. We buffer the chanel so
	// as not to slow down the fetch routine.
	Inbox chan *bucket.Bucket
	// The converter will take items from the inbox,
	// fill in the bucket with the vals, then convert the
	// bucket into a librato metric.
	Conversions chan *LibratoPayload
	// The converter will place the librato metrics into
	// the outbox for HTTP submission. We rely on the batch
	// routine to make sure that the collections of librato metrics
	// in the outbox are homogeneous with respect to their token.
	// This ensures that we route the metrics to the correct librato account.
	Outbox chan []*LibratoPayload
	// How many outlet routines should be running.
	NumOutlets int
	// How many accept routines should be running.
	NumConverters int
	Reader        Reader
	Retries       int
	User          string
	Pass          string
}

func NewLibratoOutlet(mi, mc, mo int) *LibratoOutlet {
	l := new(LibratoOutlet)
	l.Inbox = make(chan *bucket.Bucket, mi)
	l.Conversions = make(chan *LibratoPayload, mc)
	l.Outbox = make(chan []*LibratoPayload, mo)
	return l
}

func (l *LibratoOutlet) Start() {
	go l.Reader.Start(l.Inbox)
	for i := 0; i < l.NumConverters; i++ {
		go l.convert()
	}
	go l.batch()
	for i := 0; i < l.NumOutlets; i++ {
		go l.outlet()
	}
	go l.report()
}

func (l *LibratoOutlet) report() {
	for _ = range time.Tick(time.Second * 2) {
		utils.MeasureI("librato-outlet.inbox", "buckets", int64(len(l.Inbox)))
		utils.MeasureI("librato-outlet.conversions", "payloads", int64(len(l.Conversions)))
		utils.MeasureI("librato-outlet.outbox", "requests", int64(len(l.Outbox)))
	}
}

func (l *LibratoOutlet) convert() {
	for bucket := range l.Inbox {
		if len(bucket.Vals) == 0 {
			fmt.Printf("at=bucket-no-vals bucket=%s\n", bucket.Id.Name)
			continue
		}
		//TODO(ryandotsmith): This is getting out of control.
		//We need a succinct way to building payloads.
		countAttr := &LibratoAttributes{Min: 0, Units: "count"}
		attrs := &LibratoAttributes{Min: 0, Units: bucket.Id.Units}
		switch bucket.Id.User {
		case "librato-production@heroku.com", "dod@heroku.com":
			{
				sum := bucket.Sum()
				count := bucket.Count()
				min := bucket.Min()
				max := bucket.Max()
				l.Conversions <- &LibratoPayload{
					Attr:   attrs,
					User:   bucket.Id.User,
					Pass:   bucket.Id.Pass,
					Time:   ft(bucket.Id.Time),
					Source: bucket.Id.Source,
					Name:   bucket.Id.Name,
					Sum:    &sum,
					Count:  &count,
					Min:    &min,
					Max:    &max,
				}
			}
		default:
			{
				l.Conversions <- &LibratoPayload{Attr: countAttr, User: bucket.Id.User, Pass: bucket.Id.Pass, Time: ft(bucket.Id.Time), Source: bucket.Id.Source, Name: bucket.Id.Name + ".count", Val: fi(bucket.Count())}
				l.Conversions <- &LibratoPayload{Attr: attrs, User: bucket.Id.User, Pass: bucket.Id.Pass, Time: ft(bucket.Id.Time), Source: bucket.Id.Source, Name: bucket.Id.Name + ".sum", Val: ff(bucket.Sum())}
				l.Conversions <- &LibratoPayload{Attr: attrs, User: bucket.Id.User, Pass: bucket.Id.Pass, Time: ft(bucket.Id.Time), Source: bucket.Id.Source, Name: bucket.Id.Name + ".min", Val: ff(bucket.Min())}
				l.Conversions <- &LibratoPayload{Attr: attrs, User: bucket.Id.User, Pass: bucket.Id.Pass, Time: ft(bucket.Id.Time), Source: bucket.Id.Source, Name: bucket.Id.Name + ".max", Val: ff(bucket.Max())}
				l.Conversions <- &LibratoPayload{Attr: attrs, User: bucket.Id.User, Pass: bucket.Id.Pass, Time: ft(bucket.Id.Time), Source: bucket.Id.Source, Name: bucket.Id.Name + ".mean", Val: ff(bucket.Mean())}
			}
		}
		l.Conversions <- &LibratoPayload{Attr: attrs, User: bucket.Id.User, Pass: bucket.Id.Pass, Time: ft(bucket.Id.Time), Source: bucket.Id.Source, Name: bucket.Id.Name + ".median", Val: ff(bucket.Median())}
		l.Conversions <- &LibratoPayload{Attr: attrs, User: bucket.Id.User, Pass: bucket.Id.Pass, Time: ft(bucket.Id.Time), Source: bucket.Id.Source, Name: bucket.Id.Name + ".perc95", Val: ff(bucket.P95())}
		l.Conversions <- &LibratoPayload{Attr: attrs, User: bucket.Id.User, Pass: bucket.Id.Pass, Time: ft(bucket.Id.Time), Source: bucket.Id.Source, Name: bucket.Id.Name + ".perc99", Val: ff(bucket.P99())}
		l.Conversions <- &LibratoPayload{Attr: attrs, User: bucket.Id.User, Pass: bucket.Id.Pass, Time: ft(bucket.Id.Time), Source: bucket.Id.Source, Name: bucket.Id.Name + ".last", Val: ff(bucket.Last())}
		fmt.Printf("measure.bucket.conversion.delay=%d bucket.user=%q bucket.name=%q bucket.source=%q bucket.time=%d\n", bucket.Id.Delay(time.Now()), bucket.Id.User, bucket.Id.Name, bucket.Id.Source, ft(bucket.Id.Time))
	}
}

func (l *LibratoOutlet) batch() {
	ticker := time.Tick(time.Millisecond * 200)
	batchMap := make(map[string][]*LibratoPayload)
	for {
		select {
		case <-ticker:
			for k, v := range batchMap {
				if len(v) > 0 {
					l.Outbox <- v
				}
				delete(batchMap, k)
			}
		case payload := <-l.Conversions:
			index := payload.User + ":" + payload.Pass
			_, present := batchMap[index]
			if !present {
				batchMap[index] = make([]*LibratoPayload, 1, 300)
				batchMap[index][0] = payload
			} else {
				batchMap[index] = append(batchMap[index], payload)
			}
			if len(batchMap[index]) == cap(batchMap[index]) {
				l.Outbox <- batchMap[index]
				delete(batchMap, index)
			}
		}
	}
}

func (l *LibratoOutlet) outlet() {
	for payloads := range l.Outbox {
		if len(payloads) < 1 {
			fmt.Printf("at=%q\n", "empty-metrics-error")
			continue
		}
		//Since a playload contains all metrics for
		//a unique librato user/pass, we can extract the user/pass
		//from any one of the payloads.
		sample := payloads[0]
		reqBody := new(LibratoRequest)
		reqBody.Gauges = payloads
		j, err := json.Marshal(reqBody)
		if err != nil {
			fmt.Printf("at=json-error error=%s user=%s\n", err, sample.User)
			continue
		}
		l.postWithRetry(sample.User, sample.Pass, bytes.NewBuffer(j))
	}
}

func (l *LibratoOutlet) postWithRetry(u, p string, body *bytes.Buffer) error {
	for i := 0; i <= l.Retries; i++ {
		if err := l.post(u, p, body); err != nil {
			fmt.Printf("measure.librato.error user=%s msg=%s attempt=%d\n", u, err, i)
			if i == l.Retries {
				return err
			}
			continue
		}
		return nil
	}
	//Should not be possible.
	return errors.New("Unable to post.")
}

func (l *LibratoOutlet) post(u, p string, body *bytes.Buffer) error {
	defer utils.MeasureT("librato-post", time.Now())
	req, err := http.NewRequest("POST", libratoUrl, body)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("User-Agent", "l2met/0")
	req.SetBasicAuth(u, p)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		var m string
		s, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			m = fmt.Sprintf("error=failed-request code=%d", resp.StatusCode)
		} else {
			m = fmt.Sprintf("error=failed-request code=%d resp=body=%s req-body=%s",
				resp.StatusCode, s, body)
		}
		return errors.New(m)
	}
	return nil
}

func ff(x float64) string {
	return strconv.FormatFloat(x, 'f', 5, 64)
}

func fi(x int) string {
	return strconv.FormatInt(int64(x), 10)
}

func ft(t time.Time) int64 {
	return t.Unix()
}
