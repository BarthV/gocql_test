package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var set_summary = prometheus.NewSummary(prometheus.SummaryOpts{
	Name:       "set_exec_time",
	Help:       "SET execution time summary",
	Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
})

var get_summary = prometheus.NewSummary(prometheus.SummaryOpts{
	Name:       "get_exec_time",
	Help:       "GET execution time summary",
	Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
})

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var src = rand.NewSource(time.Now().UnixNano())

func set_elapsed() func() {
	t := time.Now()
	return func() {
		set_summary.Observe(float64(time.Since(t).Nanoseconds()))
	}
}

func get_elapsed() func() {
	t := time.Now()
	return func() {
		get_summary.Observe(float64(time.Since(t).Nanoseconds()))
	}
}

func RandStringBytesMaskImprSrc(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

func set_rnd_key(sess *gocql.Session) {
	defer set_elapsed()()
	key := "memtier-" + strconv.Itoa(rand.Intn(29999)+1)
	kv_qi := func(q *gocql.QueryInfo) ([]interface{}, error) {
		values := make([]interface{}, 2)
		values[0] = key
		values[1] = RandStringBytesMaskImprSrc(1024)
		// values[1] = "lalilol"
		return values, nil
	}

	if err := sess.Bind("INSERT INTO kvstore.bucket1 (keycol,valuecol) VALUES (?, ?)", kv_qi).Exec(); err != nil {
		fmt.Println(err)
	}
	// fmt.Println("SET key=" + key)
}

func get_rnd_key(sess *gocql.Session) {
	defer get_elapsed()()
	key := "memtier-" + strconv.Itoa(rand.Intn(29999)+1)
	key_qi := func(q *gocql.QueryInfo) ([]interface{}, error) {
		values := make([]interface{}, 1)
		values[0] = key
		return values, nil
	}

	var val []byte

	if err := sess.Bind("SELECT keycol,valuecol FROM kvstore.bucket1 where keycol=?", key_qi).Scan(&key, &val); err != nil {
		fmt.Println(err)
	}
	// fmt.Println("GET key=" + "memtier-" + key + " len=" + strconv.Itoa(len(val)))
}

func main() {
	prometheus.MustRegister(set_summary)
	prometheus.MustRegister(get_summary)
	var addr = flag.String("listen-address", ":8080", "The address to listen on for HTTP requests.")
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(*addr, nil)

	clust := gocql.NewCluster("cassandra.seed.hostname")
	clust.Keyspace = "kvstore"
	clust.Consistency = gocql.LocalOne
	sess, err := clust.CreateSession()
	if err != nil {
		fmt.Println(err)
		return
	}

	rate := time.Second / 40000
	throttle := time.Tick(rate)
	count := 1
	for {
		<-throttle      // rate limit our Service.Method RPCs
		if count <= 6 { // 60% read
			go set_rnd_key(sess)
		} else { // 40% write
			go get_rnd_key(sess)
		}
		if count == 10 { // reset read-write ratio counter
			count = 1
		} else {
			count += 1
		}
	}

	fmt.Println("END")
	time.Sleep(120)
}
