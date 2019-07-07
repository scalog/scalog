package order

import (
	"testing"
	"time"
)

func TestNewOrderServer(t *testing.T) {
	s := NewOrderServer(0, 1, 2, time.Second, make([]string, 0))
	if s == nil {
		t.Fatal("New OrderServer is nil")
	}
}
