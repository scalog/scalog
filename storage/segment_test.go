package storage

import (
	"os"
	"testing"
)

func check(t *testing.T, err error) {
	if err != nil {
		t.Errorf("%v", err)
	}
}

func TestNewSegment(t *testing.T) {
	record := "test"
	s, err := NewSegment("tmp", 0)
	if err != nil {
		t.Errorf("%v", err)
	}
	if s == nil {
		t.Errorf("Get nil segment on creating")
	}
	l, err := s.Write(record)
	if err != nil {
		t.Errorf("%v", err)
	}
	if l != 0 {
		t.Errorf("Write error: expect ssn %v, get %v", len(record), l)
	}
	r, err := s.ReadLSN(0)
	if err != nil {
		t.Errorf("%v", err)
	}
	if r != record {
		t.Errorf("Read error: expect '%v', get '%v'", record, r)
	}
	err = s.Assign(0, 1, 100)
	if err != nil {
		t.Errorf("%v", err)
	}
	r, err = s.ReadGSN(100)
	if err != nil {
		t.Errorf("%v", err)
	}
	if r != record {
		t.Errorf("Read error: expect '%v', get '%v'", record, r)
	}
	err = s.Close()
	if err != nil {
		t.Errorf("%v", err)
	}

	s, err = RecoverSegment("tmp", 0)
	if err != nil {
		t.Errorf("%v", err)
	}
	if s == nil {
		t.Errorf("Get nil segment on recovery")
	}
	r, err = s.ReadLSN(0)
	if err != nil {
		t.Errorf("%v", err)
	}
	if r != record {
		t.Errorf("Read error: expect '%v', get '%v'", record, r)
	}
	r, err = s.ReadGSN(100)
	if err != nil {
		t.Errorf("%v", err)
	}
	if r != record {
		t.Errorf("Read error: expect '%v', get '%v'", record, r)
	}

	err = os.RemoveAll("tmp")
	if err != nil {
		t.Errorf("%v", err)
	}
}
