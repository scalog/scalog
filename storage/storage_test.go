package storage

import (
	"fmt"
	"os"
	"testing"
)

func TestSetupStorage(t *testing.T) {
	disk, err := NewStorage("disk0")
	if err != nil {
		t.Fatalf(err.Error())
	}
	disk.Destroy()
}

func TestSingleReadAndWrite(t *testing.T) {
	disk, err := NewStorage("disk1")
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer disk.Destroy()
	expected := "Hello, World!"
	lsn, writeErr := disk.Write(expected)
	if writeErr != nil {
		t.Fatalf(writeErr.Error())
	}
	segmentSyncErr := disk.Sync()
	if segmentSyncErr != nil {
		t.Fatalf(segmentSyncErr.Error())
	}
	gsn := int64(0)
	commitErr := disk.Commit(lsn, gsn)
	if commitErr != nil {
		t.Fatalf(commitErr.Error())
	}
	globalIndexSyncErr := disk.Sync()
	if globalIndexSyncErr != nil {
		t.Fatalf(globalIndexSyncErr.Error())
	}
	actualLSN, readLSNErr := disk.ReadLSN(lsn)
	if readLSNErr != nil {
		t.Fatalf(readLSNErr.Error())
	}
	if actualLSN != expected {
		t.Fatalf(fmt.Sprintf("Expected: \"%s\", Actual: %s", expected, actualLSN))
	}
	actualGSN, readGSNErr := disk.ReadGSN(gsn)
	if readGSNErr != nil {
		t.Fatalf(readGSNErr.Error())
	}
	if actualGSN != expected {
		t.Fatalf(fmt.Sprintf("Expected: \"%s\", Actual: %s", expected, actualGSN))
	}
}

func TestMultipleReadAndWrite(t *testing.T) {
	disk, err := NewStorage("disk2")
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer disk.Destroy()
	expected0 := "Record 0"
	lsn0, writeErr0 := disk.Write(expected0)
	if writeErr0 != nil {
		t.Fatalf(writeErr0.Error())
	}
	expected1 := "Record 1"
	lsn1, writeErr1 := disk.Write(expected1)
	if writeErr1 != nil {
		t.Fatalf(writeErr1.Error())
	}
	segmentSyncErr := disk.Sync()
	if segmentSyncErr != nil {
		t.Fatalf(segmentSyncErr.Error())
	}
	gsn0 := int64(0)
	commitErr0 := disk.Commit(lsn0, gsn0)
	if commitErr0 != nil {
		t.Fatalf(commitErr0.Error())
	}
	gsn1 := int64(1)
	commitErr1 := disk.Commit(lsn1, gsn1)
	if commitErr1 != nil {
		t.Fatalf(commitErr1.Error())
	}
	globalIndexSyncErr := disk.Sync()
	if globalIndexSyncErr != nil {
		t.Fatalf(globalIndexSyncErr.Error())
	}
	actualLSN0, readLSNErr0 := disk.ReadLSN(lsn0)
	if readLSNErr0 != nil {
		t.Fatalf(readLSNErr0.Error())
	}
	if actualLSN0 != expected0 {
		t.Fatalf(fmt.Sprintf("Expected: \"%s\", Actual: %s", expected0, actualLSN0))
	}
	actualGSN0, readGSNErr0 := disk.ReadGSN(gsn0)
	if readGSNErr0 != nil {
		t.Fatalf(readGSNErr0.Error())
	}
	if actualGSN0 != expected0 {
		t.Fatalf(fmt.Sprintf("Expected: \"%s\", Actual: %s", expected0, actualGSN0))
	}
	actualLSN1, readLSNErr1 := disk.ReadLSN(lsn1)
	if readLSNErr1 != nil {
		t.Fatalf(readLSNErr1.Error())
	}
	if actualLSN1 != expected1 {
		t.Fatalf(fmt.Sprintf("Expected: \"%s\", Actual: %s", expected1, actualLSN1))
	}
	actualGSN1, readGSNErr1 := disk.ReadGSN(gsn1)
	if readGSNErr1 != nil {
		t.Fatalf(readGSNErr1.Error())
	}
	if actualGSN1 != expected1 {
		t.Fatalf(fmt.Sprintf("Expected: \"%s\", Actual: %s", expected1, actualGSN1))
	}
}

func TestStress(t *testing.T) {
	disk, err := NewStorage("disk3")
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer disk.Destroy()
	lsnToExpected := make(map[int64]string)
	for i := int64(0); i < 4096; i++ {
		expected := fmt.Sprintf("Record %d", i)
		lsn, writeErr := disk.Write(expected)
		if writeErr != nil {
			t.Fatalf(writeErr.Error())
		}
		lsnToExpected[lsn] = expected
	}
	segmentSyncErr := disk.Sync()
	if segmentSyncErr != nil {
		t.Fatalf(segmentSyncErr.Error())
	}
	for i := int64(0); i < 4096; i++ {
		commitErr := disk.Commit(i, i+2048)
		if commitErr != nil {
			t.Fatalf(commitErr.Error())
		}
	}
	globalIndexSyncErr := disk.Sync()
	if globalIndexSyncErr != nil {
		t.Fatalf(globalIndexSyncErr.Error())
	}
	for i := int64(0); i < 4096; i++ {
		actualLSN, readLSNErr := disk.ReadLSN(i)
		if readLSNErr != nil {
			t.Fatalf(readLSNErr.Error())
		}
		if actualLSN != lsnToExpected[i] {
			t.Fatalf(fmt.Sprintf("Expected: \"%s\", Actual: %s", lsnToExpected[i], actualLSN))
		}
		actualGSN, readGSNErr := disk.ReadGSN(i + 2048)
		if readGSNErr != nil {
			t.Fatalf(readGSNErr.Error())
		}
		if actualGSN != lsnToExpected[i] {
			t.Fatalf(fmt.Sprintf("Expected: \"%s\", Actual: %s", lsnToExpected[i], actualGSN))
		}
	}
}

func TestDelete(t *testing.T) {
	disk, err := NewStorage("disk4")
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer disk.Destroy()
	for i := 0; i < 2048; i++ {
		_, err := disk.Write(fmt.Sprintf("Record %d", i))
		if err != nil {
			t.Fatalf(err.Error())
		}
	}
	err = disk.Sync()
	if err != nil {
		t.Fatalf(err.Error())
	}
	for i := int64(0); i < 2048; i++ {
		err = disk.Commit(i, i+2048)
		if err != nil {
			t.Fatalf(err.Error())
		}
	}
	err = disk.Sync()
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = disk.Delete(3072)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if exists("disk4/partition0/0000000000000000000.local") {
		t.Fatalf("Failed to delete disk4/partition0/0000000000000000000.local")
	}
	if exists("disk4/partition0/0000000000000000000.log") {
		t.Fatalf("Failed to delete disk4/partition0/0000000000000000000.log")
	}
	if exists("disk4/partition0/0000000000000002048.global") {
		t.Fatalf("Failed to delete disk4/partition0/0000000000000002048.global")
	}
	if !exists("disk4/partition0/0000000000000001024.local") {
		t.Fatalf("Incorrectly deleted disk4/partition0/0000000000000001024.local")
	}
	if !exists("disk4/partition0/0000000000000001024.log") {
		t.Fatalf("Incorrectly deleted disk4/partition0/0000000000000001024.log")
	}
	if !exists("disk4/partition0/0000000000000003072.global") {
		t.Fatalf("Incorrectly deleted disk4/partition0/0000000000000003072.global")
	}
}

func exists(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}
