package store

import (
	"bufio"
	"fmt"
	"github.com/bmizerany/logplex"
	"io"
	"l2met/db"
	"l2met/encoding"
	"l2met/utils"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Bucket struct {
	sync.Mutex
	Id     int64     `json:"id"`
	Time   time.Time `json:"time"`
	Name   string    `json:"name"`
	Source string    `json:"source,omitempty"`
	Token  string
	Vals   []float64 `json:"vals,omitempty"`
}

// Cachable Interface
func (b *Bucket) Key() int64 {
	return b.Id
}

func FindMetrics(min, max time.Time) ([]int64, error) {
	var metrics []int64
	startQuery := time.Now()
	r, err := db.PGR.Query("select id from metrics where bucket >= $1 and bucket < $2 order by bucket desc",
		min, max)
	if err != nil {
		return nil, err
	}
	utils.MeasureT(startQuery, "metrics.query")
	startParse := time.Now()
	defer r.Close()
	for r.Next() {
		var id int64
		r.Scan(&id)
		metrics = append(metrics, id)
	}
	utils.MeasureT(startParse, "metrics.vals.parse")
	return metrics, nil
}

func NewBucket(token string, rdr *bufio.Reader) ([]*Bucket, error) {
	var buckets []*Bucket
	lp := logplex.NewReader(rdr)
	for {
		packet, err := lp.ReadMsg()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("at=logplex-error err=%s\n", err)
			return nil, err
		}

		d, err := encoding.ParseMsgData(packet.Msg)
		if err != nil {
			continue
		}

		name, ok := d["measure"]
		if !ok {
			continue
		}

		source, ok := d["source"]
		if !ok {
			source = ""
		}

		var val float64
		tmpVal, ok := d["val"]
		if ok {
			val, err = strconv.ParseFloat(tmpVal, 64)
			if err != nil {
				fmt.Printf("at=error error=\"unable to parse val.\"\n")
				continue
			}
		} else {
			val = float64(1)
		}

		t, err := packet.Time()
		if err != nil {
			fmt.Printf("at=time-error error=%s\n", err)
			continue
		}

		m := &Bucket{}
		m.Token = token
		m.Time = utils.RoundTime(t, time.Minute)
		m.Name = name
		m.Source = source
		m.Vals = append(m.Vals, val)
		buckets = append(buckets, m)
	}
	return buckets, nil
}

func (b *Bucket) Add(otherM *Bucket) {
	b.Lock()
	defer b.Unlock()
	for _, v := range otherM.Vals {
		b.Vals = append(b.Vals, v)
	}
}

// time:consumer:name:source
func (b *Bucket) String() (res string) {
	b.Lock()
	defer b.Unlock()
	res += strconv.FormatInt(b.Time.Unix(), 10) + ":"
	res += b.Name
	if len(b.Source) > 0 {
		res += ":" + b.Source
	}
	return
}

func (b *Bucket) Get() {
	db.PGRLocker.Lock()
	rows, err := db.PGR.Query("select name, bucket, token, vals from metrics where id = $1",
		b.Id)
	if err != nil {
		fmt.Printf("at=error error=%s\n", err)
		return
	}
	rows.Next()
	var tmp []byte
	rows.Scan(&b.Name, &b.Time, &b.Token, &tmp)
	rows.Close()
	db.PGRLocker.Unlock()

	if len(tmp) == 0 {
		b.Vals = []float64{}
		return
	}
	encoding.DecodeArray(tmp, &b.Vals)
}

func (b *Bucket) dbId() (int64, error) {
	rows, err := db.PG.Query("select id from metrics where name = $1 and bucket = $2",
		b.Name, b.Time)
	if err != nil {
		fmt.Printf("at=error error=%s\n", err)
		return 0, err
	}
	// Grab the first row.
	rows.Next()
	var id int64
	rows.Scan(&id)
	rows.Close()
	return id, nil
}

func (b *Bucket) Put() (int64, error) {
	b.Lock()
	defer b.Unlock()
	db.PGLocker.Lock()
	defer db.PGLocker.Unlock()
	var err error
	id, _ := b.dbId()
	// Create the bucket if needed.
	if id == 0 {
		fmt.Printf("at=create-bucket name=%s bucket=%s\n",
			b.Name, b.Time)
		_, err := db.PG.Exec("insert into metrics (name, bucket, token) values($1,$2,$3)",
			b.Name, b.Time, b.Token)
		if err != nil {
			fmt.Printf("at=error error=%s\n", err)
			return 0, err
		}
	}

	res, err := db.PG.Exec("update metrics set vals = vals || $1 where name = $2 and bucket = $3",
		string(encoding.EncodeArray(b.Vals)), b.Name, b.Time)
	if err != nil {
		fmt.Printf("at=error error=%s\n", err)
		return 0, err
	}

	//Reset the vals on this bucket. It might be used again.
	// It might be used again.
	b.Vals = []float64{}

	var count int64
	count, err = res.RowsAffected()
	if err != nil {
		fmt.Printf("at=error error=%s\n", err)
		count = 0
	}
	return count, nil
}

func (b *Bucket) Count() int {
	return len(b.Vals)
}

func (b *Bucket) Sum() float64 {
	s := float64(0)
	for i := range b.Vals {
		s += b.Vals[i]
	}
	return s
}

func (b *Bucket) Mean() float64 {
	return b.Sum() / float64(b.Count())
}

func (b *Bucket) Sort() {
	if !sort.Float64sAreSorted(b.Vals) {
		sort.Float64s(b.Vals)
	}
}

func (b *Bucket) Min() float64 {
	b.Sort()
	return b.Vals[0]
}

func (b *Bucket) Median() float64 {
	b.Sort()
	pos := int(math.Ceil(float64(b.Count() / 2)))
	return b.Vals[pos]
}

func (b *Bucket) P95() float64 {
	b.Sort()
	pos := int(math.Floor(float64(b.Count()) * 0.95))
	return b.Vals[pos]
}

func (b *Bucket) P99() float64 {
	b.Sort()
	pos := int(math.Floor(float64(b.Count()) * 0.99))
	return b.Vals[pos]
}

func (b *Bucket) Max() float64 {
	b.Sort()
	pos := b.Count() - 1
	return b.Vals[pos]
}