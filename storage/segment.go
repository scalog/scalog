package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
)

const LogMetaDataLength = 4

type Segment struct {
	baseLSN int64
	baseGSN int64
	nextSSN int32
	logPos  int32
	logFile *os.File
	ssnMap  map[int32]int32
	gsnMap  map[int32]int32

	tmp []byte // reuse this across the lifetime of the segment
}

func NewSegment(baseLSN int64) (*Segment, error) {
	var err error
	s := &Segment{baseLSN: baseLSN, nextSSN: 0, logPos: 0}
	s.tmp = make([]byte, LogMetaDataLength)
	s.logFile, err = os.Create(fmt.Sprintf("%v.log", baseLSN))
	if err != nil {
		return nil, err
	}
	return s, nil
}

func RecoverSegment(baseLSN int64) (*Segment, error) {
	s := &Segment{baseLSN: baseLSN}
	// read the log file and reconstruct content for ssnFile and gsnFile
	file, err := os.OpenFile(fmt.Sprintf("%v.log", baseLSN), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	b := make([]byte, 1024*1024) // maximum record size is 1MB
	for i := int32(0); ; i++ {
		l, err := file.Read(s.tmp)
		if l != LogMetaDataLength {
			return nil, fmt.Errorf("Read log file %v error: expect length %v, get %v", baseLSN, LogMetaDataLength, l)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		ll := binary.LittleEndian.Uint32(s.tmp)
		l, err = file.Read(b[:ll])
		if l != int(ll) {
			return nil, fmt.Errorf("Read log file %v error: expect length %v, get %v", baseLSN, ll, l)
		}
		if err == io.EOF {
			return nil, fmt.Errorf("Unexpected EOF when reading log file %v error", baseLSN)
		}
		if err != nil {
			return nil, err
		}
		s.ssnMap[i] = s.logPos
		s.logPos += 4 + int32(ll)
	}
	// open the file in append mode to continue functioning
	s.logFile, err = os.OpenFile(fmt.Sprintf("%v.log", baseLSN), os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Segment) Write(record string) (int32, error) {
	var err error
	ssn := s.nextSSN
	s.ssnMap[ssn] = s.logPos
	// each record is structured as length+record
	l := int32(len(record))
	s.logPos += LogMetaDataLength + l
	s.nextSSN++
	binary.LittleEndian.PutUint32(s.tmp, uint32(l))
	_, err = s.logFile.Write(s.tmp)
	if err != nil {
		return 0, err
	}
	_, err = s.logFile.WriteString(record)
	if err != nil {
		return 0, err
	}
	return ssn, nil
}

func (s *Segment) Assign(ssn, length int32, gsn int64) {
	gsnOffset := int32(gsn - s.baseGSN)
	for i := int32(0); i < length; i++ {
		s.gsnMap[gsnOffset+i] = s.ssnMap[ssn+i]
	}
}

func writeMapToDisk(f string, m map[int32]int32) error {
	file, err := os.Create(f)
	if err != nil {
		return err
	}
	defer file.Close()
	// sort by keys
	keys := make([]int, len(m))
	i := 0
	for k, _ := range m {
		keys[i] = int(k)
		i++
	}
	sort.Ints(keys)
	// write the map to file
	b := make([]byte, 8)
	for _, k := range keys {
		binary.LittleEndian.PutUint32(b[0:4], uint32(k))
		binary.LittleEndian.PutUint32(b[4:8], uint32(m[int32(k)]))
		_, err := file.Write(b)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Segment) Close() error {
	err := s.logFile.Close()
	if err != nil {
		return err
	}
	err = writeMapToDisk(fmt.Sprintf("%v.ssn", s.baseLSN), s.ssnMap)
	if err != nil {
		return err
	}
	err = writeMapToDisk(fmt.Sprintf("%v.gsn", s.baseLSN), s.ssnMap)
	if err != nil {
		return err
	}
	return nil
}
