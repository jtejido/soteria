package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
	"github.com/jtejido/soteria"
)

var cb *soteria.CircuitBreaker

func init() {
	var st soteria.Settings
	st.Name = "HTTP GET"
	st.ReadyToTrip = func(stats soteria.Stats) bool {
		failureRatio := float64(stats.TotalFailures) / float64(stats.Requests)
		return stats.Requests >= 3 && failureRatio >= 0.6
	}

	cb = soteria.New(st)
}

// Get wraps http.Get in CircuitBreaker.
func Get(url string) ([]byte, error) {
	body, err := cb.Execute(func() (interface{}, error) {
		resp, err := http.Get(url)
		if err != nil {
			return nil, err
		}

		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		return body, nil
	})
	fmt.Println(cb)
	time.Sleep(2 * time.Second)
	
	// time.Sleep(61 * time.Second)
	body2, err2 := cb.Execute(func() (interface{}, error) {
		resp, err := http.Get(url)
		if err != nil {
			return nil, err
		}

		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		return body, nil
	})
	fmt.Println(cb)

	if err2 != nil {
		return nil, err
	}

	return body2.([]byte), nil


	if err != nil {
		return nil, err
	}

	return body.([]byte), nil
}

func main() {
	body, err := Get("http://www.asdasdasfdsddfg.com/robots.txt")
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(string(body))
}