package order

import (
	"testing"
)

func TestNewOrderServer(t *testing.T) {
	s := NewOrderServer()
	if s == nil {
		t.Fatal("New OrderServer is nil")
	}
}
