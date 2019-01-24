package soteria

import (
	"github.com/jtejido/persephone"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const (
	StateClosed persephone.State = iota
	StateHalfOpen
	StateOpen
)

const (
	Ok persephone.Input = iota
	NotOk
)

const defaultTimeout = time.Duration(60) * time.Second

var (
	ErrTooManyRequests = errors.New("too many requests")
	ErrOpenState = errors.New("circuit breaker is open")
	states persephone.States
	inputs persephone.Inputs
)

type Stats struct {
	Requests             uint32
	TotalSuccesses       uint32
	TotalFailures        uint32
	ConsecutiveSuccesses uint32
	ConsecutiveFailures  uint32
}

func (c *Stats) request() {
	atomic.AddUint32(&c.Requests, 1)
}

func (c *Stats) success() {
	atomic.AddUint32(&c.TotalSuccesses, 1)
	atomic.AddUint32(&c.ConsecutiveSuccesses, 1)
	atomic.AddUint32(&c.ConsecutiveFailures, -c.ConsecutiveFailures)

}

func (c *Stats) failure() {
	atomic.AddUint32(&c.TotalFailures, 1)
	atomic.AddUint32(&c.ConsecutiveFailures, 1)
	atomic.AddUint32(&c.ConsecutiveSuccesses, -c.ConsecutiveSuccesses)
}

func (c *Stats) clear() {
	atomic.AddUint32(&c.Requests, -c.Requests)
	atomic.AddUint32(&c.TotalSuccesses, -c.TotalSuccesses)
	atomic.AddUint32(&c.TotalFailures, -c.TotalFailures)
	atomic.AddUint32(&c.ConsecutiveSuccesses, -c.ConsecutiveSuccesses)
	atomic.AddUint32(&c.ConsecutiveFailures, -c.ConsecutiveFailures)
}

// Settings configures CircuitBreaker:
//
// Name is the name of the CircuitBreaker.
//
// MaxRequests is the maximum number of requests allowed to pass through
// when the CircuitBreaker is half-open.
// If MaxRequests is 0, the CircuitBreaker allows only 1 request.
//
// Interval is the cyclic period of the closed state
// for the CircuitBreaker to clear the internal Counts.
// If Interval is 0, the CircuitBreaker doesn't clear internal Counts during the closed state.
//
// Timeout is the period of the open state,
// after which the state of the CircuitBreaker becomes half-open.
// If Timeout is 0, the timeout value of the CircuitBreaker is set to 60 seconds.
//
// ReadyToTrip is called with a copy of Counts whenever a request fails in the closed state.
// If ReadyToTrip returns true, the CircuitBreaker will be placed into the open state.
// If ReadyToTrip is nil, default ReadyToTrip is used.
// Default ReadyToTrip returns true when the number of consecutive failures is more than 5.
type Settings struct {
	Name          string
	MaxRequests   uint32
	Interval      time.Duration
	Timeout       time.Duration
	ReadyToTrip   func(stats Stats) bool
}

type CircuitBreaker struct {
	name          string
	maxRequests   uint32
	interval      time.Duration
	timeout       time.Duration
	readyToTrip   func(stats Stats) bool

	mutex      sync.Mutex
	generation uint64
	stats     Stats
	expiry     time.Time
	*persephone.AbstractFSM
}

func New(settings Settings) *CircuitBreaker {

	cb := new(CircuitBreaker)

	// add states
	states.Add(StateClosed, persephone.INITIAL_STATE)
	states.Add(StateHalfOpen, persephone.NORMAL_STATE)
	states.Add(StateOpen, persephone.NORMAL_STATE)

	// add inputs
	inputs.Add(Ok)
	inputs.Add(NotOk)

	// initialize FSM
	cb.AbstractFSM = persephone.New(states, inputs)

	cb.name = settings.Name
	cb.interval = settings.Interval

	if settings.MaxRequests == 0 {
		cb.maxRequests = 1
	} else {
		cb.maxRequests = settings.MaxRequests
	}

	if settings.Timeout == 0 {
		cb.timeout = defaultTimeout
	} else {
		cb.timeout = settings.Timeout
	}

	if settings.ReadyToTrip == nil {
		cb.readyToTrip = defaultReadyToTrip
	} else {
		cb.readyToTrip = settings.ReadyToTrip
	}

	cb.generate(time.Now())
	cb.init()
	return cb
}

func defaultReadyToTrip(stats Stats) bool {
	return stats.ConsecutiveFailures > 5
}

func (cb *CircuitBreaker) init() {
	// Add rules, you can choose to add a method as an input action for a src => input map.
	// Alternatively, you can separate it via cb.AddInputAction(src, input, func() error)
	cb.AddRule(StateClosed, NotOk, StateOpen, cb.ClosedNotOkAction)
	cb.AddRule(StateClosed, Ok, StateClosed, cb.ClosedOkAction)
	cb.AddRule(StateOpen, NotOk, StateOpen, nil)
	cb.AddRule(StateHalfOpen, Ok, StateClosed, cb.HalfOpenOkAction)
	cb.AddRule(StateHalfOpen, NotOk, StateOpen, cb.HalfOpenNotOkAction)

}

func (cb *CircuitBreaker) Name() string {
	return cb.name
}

func (cb *CircuitBreaker) State() persephone.State {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	cb.currentState(now)
	return cb.GetState()
}

func (cb *CircuitBreaker) Execute(req func() (interface{}, error)) (interface{}, error) {
	var now time.Time
	now = time.Now()
	cb.currentState(now)

	if cb.GetState() == StateOpen {
		return nil, ErrOpenState
	}

	if cb.GetState() == StateHalfOpen && cb.stats.Requests >= cb.maxRequests {
		return nil, ErrTooManyRequests
	}

	cb.stats.request()

	result, err := req()

	if err != nil {
		err_f := cb.Process(NotOk)
		if err_f != nil {
			return result, err_f
		}

		return result, err
	}

	err_t := cb.Process(Ok)	

	if err_t != nil {
		return result, err_t
	}

	return result, err
}

func (cb *CircuitBreaker) ClosedOkAction() error {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()
	cb.stats.success()
	return nil
}

func (cb *CircuitBreaker) HalfOpenOkAction() error {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()
	cb.stats.success()
	if cb.stats.ConsecutiveSuccesses >= cb.maxRequests {
		now := time.Now()
		cb.generate(now)
	}
	return nil
}

func (cb *CircuitBreaker) ClosedNotOkAction() error {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()
	cb.stats.failure()
	if cb.readyToTrip(cb.stats) {
		now := time.Now()
		cb.generate(now)
	}
	return nil
}

func (cb *CircuitBreaker) HalfOpenNotOkAction() error {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()
	now := time.Now()
	cb.generate(now)
	return nil
}

func (cb *CircuitBreaker) currentState(now time.Time) {
	switch cb.GetState() {
	case StateClosed:
		if !cb.expiry.IsZero() && cb.expiry.Before(now) {
			cb.generate(now)
		}
	case StateOpen:
		if cb.expiry.Before(now) {
			cb.generate(now)
		}
	}
}

func (cb *CircuitBreaker) generate(now time.Time) {
	cb.generation++
	cb.stats.clear()

	var zero time.Time
	switch cb.GetState() {
	case StateClosed:
		if cb.interval == 0 {
			cb.expiry = zero
		} else {
			cb.expiry = now.Add(cb.interval)
		}
	case StateOpen:
		cb.expiry = now.Add(cb.timeout)
	default:
		cb.expiry = zero
	}
}