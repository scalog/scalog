package storage

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"

	log "github.com/scalog/scalogger/logger"
)

/*
Storage handles all I/O operations to disk for a data server replica.
Storage's use of partitions and segments is modeled closely after Kafka's
storage system.
*/
type Storage struct {
	// path to storage directory
	storagePath string
	// next local sequence number to be assigned to record
	nextLSN int64
	// ID to be assigned to next partition added
	nextPartitionID int32
	// partitionID to partition
	partitions map[int32]*partition
}

/*
partition is Scalog's unit of storage. A partition is an ordered sequence
of entries that is appended to. A partition is represented as a directory
split into segments.
*/
type partition struct {
	// path to partition directory
	partitionPath string
	// ID assigned to partition
	partitionID int32
	// max number of entries for a single segment
	maxSegmentSize int32
	// segment that will be written to
	activeSegment *segment
	// baseOffset to segment
	segments map[int64]*segment
	// global index file that will be written to
	activeGlobalIndex *globalIndex
	// first gsn in global index file to global index file
	globalIndexes map[int64]*globalIndex
}

/*
segment is a continuous subsection of a partition. A segment is represented
as an index file and a log file.
*/
type segment struct {
	// offset of first entry in segment
	baseOffset int64
	// relativeOffset to be assigned to next entry
	nextRelativeOffset int32
	// position to be assigned to next entry
	nextPosition int32
	// log file
	log *os.File
	// local index file
	localIndex *os.File
	// writer for log file
	logWriter *bufio.Writer
	// writer for local index file
	localIndexWriter *bufio.Writer
}

/*
globalIndex is an index mapping an entry's global sequence number to its
position in a segment's log file.
*/
type globalIndex struct {
	// first gsn in global index file
	startGsn          int64
	size              int32
	globalIndex       *os.File
	globalIndexWriter *bufio.Writer
}

/*
logEntry is a single entry in a segment's log file.
*/
type logEntry struct {
	Length int32
	Record string
}

const logSuffix = ".log"
const localIndexSuffix = ".local"
const globalIndexSuffix = ".global"

/*
NewStorage creates a new directory at [storagePath] and returns a new instance
of storage for creating partitions and writing to them.
*/
func NewStorage(storagePath string) (*Storage, error) {
	err := os.MkdirAll(storagePath, os.ModePerm)
	if err != nil {
		log.Printf(err.Error())
		return nil, err
	}
	s := &Storage{
		storagePath:     storagePath,
		nextLSN:         0,
		nextPartitionID: 0,
		partitions:      make(map[int32]*partition),
	}
	_, err = s.addPartition()
	if err != nil {
		log.Printf(err.Error())
		return nil, err
	}
	return s, nil
}

/*
Write writes an entry to the default partition and returns the local sequence number.
*/
func (s *Storage) Write(record string) (int64, error) {
	lsn := s.nextLSN
	err := s.WriteToPartition(s.nextPartitionID-1, lsn, record)
	if err != nil {
		log.Printf(err.Error())
		return -1, err
	}
	s.nextLSN++
	return lsn, err
}

/*
ReadLSN reads an entry with local sequence number [lsn] from the default partition.
*/
func (s *Storage) ReadLSN(lsn int64) (string, error) {
	record, err := s.ReadLSNFromPartition(s.nextPartitionID-1, lsn)
	if err != nil {
		log.Printf(err.Error())
		return "", err
	}
	return record, nil
}

/*
ReadGSN reads an entry with global sequence number [gsn] from the default partition.
*/
func (s *Storage) ReadGSN(gsn int64) (string, error) {
	record, err := s.ReadGSNFromPartition(s.nextPartitionID-1, gsn)
	if err != nil {
		log.Printf(err.Error())
		return "", err
	}
	return record, nil
}

/*
Commit writes an entry with local sequence number [lsn] and global sequence number
[gsn] to the appropriate global index file.
*/
func (s *Storage) Commit(lsn int64, gsn int64) error {
	err := s.commitToPartition(s.nextPartitionID-1, lsn, gsn)
	if err != nil {
		log.Printf(err.Error())
		return err
	}
	return nil
}

/*
Delete deletes the logs, local indexes, and global indexes for all records with
gsn < [gsn].
*/
func (s *Storage) Delete(gsn int64) error {
	err := s.deleteFromPartition(s.nextPartitionID-1, gsn)
	if err != nil {
		log.Printf(err.Error())
		return err
	}
	return nil
}

/*
Sync commits the storage's in-memory copy of recently written files to disk.
*/
func (s *Storage) Sync() error {
	for _, p := range s.partitions {
		if p.activeSegment != nil {
			err := p.activeSegment.sync()
			if err != nil {
				log.Printf(err.Error())
				return err
			}
		}
		if p.activeGlobalIndex != nil {
			err := p.activeGlobalIndex.sync()
			if err != nil {
				log.Printf(err.Error())
				return err
			}
		}
	}
	return nil
}

/*
Destroy deletes all files and directories associated with the storage instance.
*/
func (s *Storage) Destroy() error {
	return os.RemoveAll(s.storagePath)
}

/*
addPartition adds a new partition to storage and returns the partition's id.
*/
func (s *Storage) addPartition() (int32, error) {
	p := newPartition(s.storagePath, s.nextPartitionID)
	err := os.MkdirAll(p.partitionPath, os.ModePerm)
	if err != nil {
		return -1, err
	}
	s.partitions[p.partitionID] = p
	s.nextPartitionID++
	return p.partitionID, nil
}

/*
WriteToPartition writes an entry to partition with id [partitionID].
*/
func (s *Storage) WriteToPartition(partitionID int32, lsn int64, record string) error {
	p, in := s.partitions[partitionID]
	if !in {
		return fmt.Errorf("Attempted to write to non-existant partition %d", partitionID)
	}
	return p.WriteToActiveSegment(lsn, record)
}

func (p *partition) WriteToActiveSegment(lsn int64, record string) error {
	if p.activeSegment == nil || p.activeSegment.nextRelativeOffset >= p.maxSegmentSize ||
		lsn > p.activeSegment.baseOffset+int64(p.maxSegmentSize) {
		err := p.addActiveSegment(lsn)
		if err != nil {
			return err
		}
	}
	return p.activeSegment.WriteToSegment(lsn, record)
}

func (p *partition) addActiveSegment(lsn int64) error {
	if p.activeSegment != nil {
		err := p.activeSegment.finalize()
		if err != nil {
			return err
		}

	}
	activeSegment, err := newSegment(p.partitionPath, lsn)
	if err != nil {
		return err
	}
	p.segments[activeSegment.baseOffset] = activeSegment
	p.activeSegment = activeSegment
	return nil
}

func (s *segment) WriteToSegment(lsn int64, record string) error {
	logEntry := newLogEntry(record)
	bytesWritten, err := s.logWriter.WriteString(logEntry + "\n")
	if err != nil {
		return err
	}
	buffer := make([]byte, 8)
	binary.LittleEndian.PutUint32(buffer[0:], uint32(s.nextRelativeOffset))
	binary.LittleEndian.PutUint32(buffer[4:], uint32(s.nextPosition))
	for _, b := range buffer {
		err := s.localIndexWriter.WriteByte(b)
		if err != nil {
			return err
		}
	}
	s.nextRelativeOffset++
	s.nextPosition += int32(bytesWritten)
	return nil
}

func (s *Storage) ReadLSNFromPartition(partitionID int32, lsn int64) (string, error) {
	p, in := s.partitions[partitionID]
	if !in {
		return "", fmt.Errorf("Attempted to read from non-existant partition %d", partitionID)
	}
	segment, err := p.getSegmentContainingLSN(lsn)
	if err != nil {
		return "", err
	}
	return segment.ReadFromSegment(int32(lsn - segment.baseOffset))
}

func (p *partition) getSegmentContainingLSN(lsn int64) (*segment, error) {
	for baseOffset := range p.segments {
		if lsn >= baseOffset && lsn < baseOffset+int64(p.maxSegmentSize) {
			return p.segments[baseOffset], nil
		}
	}
	return nil, fmt.Errorf("Failed to find segment containing entry with lsn %d", lsn)
}

func (s *segment) ReadFromSegment(relativeOffset int32) (string, error) {
	position, err := getPositionOfRelativeOffset(s.localIndex.Name(), relativeOffset)
	if err != nil {
		return "", err
	}
	record, err := getRecordAtPosition(s.log.Name(), position)
	if err != nil {
		return "", err
	}
	return record, nil
}

func getPositionOfRelativeOffset(indexPath string, relativeOffset int32) (int32, error) {
	buffer, err := ioutil.ReadFile(indexPath)
	if err != nil {
		return -1, err
	}
	left := 0
	right := len(buffer) / 8
	for left < right {
		target := left + ((right - left) / 2)
		targetOffset := int32(binary.LittleEndian.Uint32(buffer[target*8:]))
		if relativeOffset == targetOffset {
			return int32(binary.LittleEndian.Uint32(buffer[target*8+4:])), nil
		} else if relativeOffset > targetOffset {
			left = target
		} else {
			right = target
		}
	}
	return -1, fmt.Errorf("Failed to find entry with relative offset %d", relativeOffset)
}

func getRecordAtPosition(logPath string, position int32) (string, error) {
	log, err := os.Open(logPath)
	if err != nil {
		return "", err
	}
	logReader := bufio.NewReader(log)
	_, err = logReader.Discard(int(position))
	if err != nil {
		return "", err
	}
	line, _, err := logReader.ReadLine()
	if err != nil {
		return "", err
	}
	logEntry := logEntry{}
	err = json.Unmarshal(line, &logEntry)
	if err != nil {
		return "", err
	}
	return logEntry.Record, nil
}

func (s *Storage) ReadGSNFromPartition(partitionID int32, gsn int64) (string, error) {
	p, in := s.partitions[partitionID]
	if !in {
		return "", fmt.Errorf("Attempted to read from non-existant partition %d", partitionID)
	}
	g, err := p.getGlobalIndexContainingGSN(gsn)
	if err != nil {
		return "", err
	}
	baseOffset, position, err := getBaseOffsetAndPositionOfGSN(g.globalIndex.Name(), gsn, g.startGsn)
	if err != nil {
		return "", err
	}
	record, err := getRecordAtPosition(p.segments[baseOffset].log.Name(), position)
	if err != nil {
		return "", err
	}
	return record, nil
}

func (p *partition) getGlobalIndexContainingGSN(gsn int64) (*globalIndex, error) {
	for startGsn := range p.globalIndexes {
		if gsn >= startGsn && gsn < startGsn+int64(p.maxSegmentSize) {
			return p.globalIndexes[startGsn], nil
		}
	}
	return nil, fmt.Errorf("Failed to find global index containing gsn %d", gsn)
}

func (p *partition) getClosestGlobalIndexContainingGSN(gsn int64) (*globalIndex, bool, error) {
	closest := int64(-1)
	for startGsn := range p.globalIndexes {
		if gsn >= startGsn {
			if gsn < startGsn+int64(p.maxSegmentSize) {
				return p.globalIndexes[startGsn], true, nil
			}
			closest = max(closest, startGsn)
		}
	}
	if closest != -1 {
		return p.globalIndexes[closest], false, nil
	}
	return nil, false, fmt.Errorf("Failed to find global index containing gsn %d", gsn)
}

func getBaseOffsetAndPositionOfGSN(globalIndexPath string, gsn int64, startGsn int64) (int64, int32, error) {
	buffer, err := ioutil.ReadFile(globalIndexPath)
	if err != nil {
		return -1, -1, err
	}
	left := 0
	right := len(buffer) / 16
	for left < right {
		target := left + ((right - left) / 2)
		targetGsn := int64(binary.LittleEndian.Uint32(buffer[target*16:])) + startGsn
		if gsn == targetGsn {
			baseOffset := int64(binary.LittleEndian.Uint64(buffer[target*16+4:]))
			position := int32(binary.LittleEndian.Uint32(buffer[target*16+12:]))
			return baseOffset, position, nil
		} else if gsn > targetGsn {
			left = target
		} else {
			right = target
		}
	}
	return -1, -1, fmt.Errorf("Failed to find gsn %d", gsn)
}

func (s *Storage) commitToPartition(partitionID int32, lsn int64, gsn int64) error {
	p, in := s.partitions[partitionID]
	if !in {
		return fmt.Errorf("Attempted to commit to non-existant partition %d", partitionID)
	}
	return p.commitToActiveGlobalIndex(lsn, gsn)
}

func (p *partition) commitToActiveGlobalIndex(lsn int64, gsn int64) error {
	if p.activeGlobalIndex == nil || p.activeGlobalIndex.size >= p.maxSegmentSize ||
		gsn > p.activeGlobalIndex.startGsn+int64(p.maxSegmentSize) {
		err := p.addActiveGlobalIndex(gsn)
		if err != nil {
			return err
		}
	}
	segment, err := p.getSegmentContainingLSN(lsn)
	if err != nil {
		return err
	}
	position, err := getPositionOfRelativeOffset(segment.localIndex.Name(), int32(lsn-segment.baseOffset))
	if err != nil {
		return err
	}
	return p.activeGlobalIndex.commit(gsn-p.activeGlobalIndex.startGsn, segment.baseOffset, position)
}

func (p *partition) addActiveGlobalIndex(gsn int64) error {
	if p.activeGlobalIndex != nil {
		err := p.activeGlobalIndex.finalize()
		if err != nil {
			return err
		}
	}
	activeGlobalIndex, err := newGlobalIndex(p.partitionPath, gsn)
	if err != nil {
		return err
	}
	p.globalIndexes[activeGlobalIndex.startGsn] = activeGlobalIndex
	p.activeGlobalIndex = activeGlobalIndex
	return nil
}

func (g *globalIndex) commit(relativeGsn int64, baseOffset int64, position int32) error {
	buffer := make([]byte, 16)
	binary.LittleEndian.PutUint32(buffer[0:], uint32(relativeGsn))
	binary.LittleEndian.PutUint64(buffer[4:], uint64(baseOffset))
	binary.LittleEndian.PutUint32(buffer[12:], uint32(position))
	for _, b := range buffer {
		err := g.globalIndexWriter.WriteByte(b)
		if err != nil {
			return err
		}
	}
	g.size++
	return nil
}

func (s *Storage) deleteFromPartition(partitionID int32, gsn int64) error {
	p, in := s.partitions[partitionID]
	if !in {
		return fmt.Errorf("Attempted to delete from non-existant partition %d", partitionID)
	}
	g, in, err := p.getClosestGlobalIndexContainingGSN(gsn)
	if err != nil {
		return err
	}
	err = p.deleteGlobalIndexes(g.startGsn)
	if err != nil {
		return err
	}
	var baseOffset int64
	if in {
		baseOffset, _, err = getBaseOffsetAndPositionOfGSN(g.globalIndex.Name(), gsn, g.startGsn)
		if err != nil {
			return err
		}
	} else {
		baseOffset, err = g.getClosestBaseOffsetOfGSN(gsn)
		if err != nil {
			return err
		}
	}
	err = p.deleteSegments(baseOffset)
	if err != nil {
		return err
	}
	return nil
}

func (p *partition) deleteGlobalIndexes(threshold int64) error {
	trimmed := make([]int64, 64)
	for startGsn, g := range p.globalIndexes {
		if startGsn < threshold {
			err := os.Remove(g.globalIndex.Name())
			if err != nil {
				return err
			}
			trimmed = append(trimmed, startGsn)
		}
	}
	for _, startGsn := range trimmed {
		delete(p.globalIndexes, startGsn)
	}
	return nil
}

func (g *globalIndex) getClosestBaseOffsetOfGSN(gsn int64) (int64, error) {
	buffer, err := ioutil.ReadFile(g.globalIndex.Name())
	if err != nil {
		return -1, err
	}
	closest := int64(-1)
	left := 0
	right := len(buffer) / 16
	for left < right {
		target := left + ((right - left) / 2)
		targetGsn := int64(binary.LittleEndian.Uint32(buffer[target*16:])) + g.startGsn
		if gsn > targetGsn {
			left = target
		} else {
			right = target
		}
	}
	return closest, nil

}

func (p *partition) deleteSegments(threshold int64) error {
	trimmed := make([]int64, 64)
	for baseOffset, s := range p.segments {
		if baseOffset < threshold {
			err := os.Remove(s.log.Name())
			if err != nil {
				return err
			}
			err = os.Remove(s.localIndex.Name())
			if err != nil {
				return err
			}
			trimmed = append(trimmed, baseOffset)
		}
	}
	for _, baseOffset := range trimmed {
		delete(p.segments, baseOffset)
	}
	return nil
}

func (g *globalIndex) finalize() error {
	err := g.sync()
	if err != nil {
		return err
	}
	err = g.globalIndex.Close()
	if err != nil {
		return err
	}
	return nil
}

func (g *globalIndex) sync() error {
	err := g.globalIndexWriter.Flush()
	if err != nil {
		log.Printf(err.Error())
		return err
	}
	err = g.globalIndex.Sync()
	if err != nil {
		log.Printf(err.Error())
		return err
	}
	return nil
}

func (s *segment) finalize() error {
	err := s.sync()
	if err != nil {
		return err
	}
	err = s.log.Close()
	if err != nil {
		return err
	}
	err = s.localIndex.Close()
	if err != nil {
		return err
	}
	return nil
}

func (s *segment) sync() error {
	err := s.logWriter.Flush()
	if err != nil {
		return err
	}
	err = s.log.Sync()
	if err != nil {
		return err
	}
	err = s.localIndexWriter.Flush()
	if err != nil {
		return err
	}
	err = s.localIndex.Sync()
	if err != nil {
		return err
	}
	return nil
}

func newPartition(storagePath string, partitionID int32) *partition {
	partitionPath := path.Join(storagePath, fmt.Sprintf("partition%d", partitionID))
	p := &partition{
		partitionPath:     partitionPath,
		partitionID:       partitionID,
		maxSegmentSize:    1024,
		activeSegment:     nil,
		segments:          make(map[int64]*segment),
		activeGlobalIndex: nil,
		globalIndexes:     make(map[int64]*globalIndex),
	}
	return p
}

func newGlobalIndex(partitionPath string, startGsn int64) (*globalIndex, error) {
	globalIndexName := getGlobalIndexName(startGsn)
	globalIndexPath := path.Join(partitionPath, globalIndexName)
	f, err := os.Create(globalIndexPath)
	if err != nil {
		return nil, err
	}
	g := &globalIndex{
		startGsn:          startGsn,
		size:              0,
		globalIndex:       f,
		globalIndexWriter: bufio.NewWriter(f),
	}
	return g, nil
}

func newSegment(partitionPath string, baseOffset int64) (*segment, error) {
	log, logWriter, err := newLog(partitionPath, baseOffset)
	if err != nil {
		return nil, err
	}
	index, indexWriter, err := newLocalIndex(partitionPath, baseOffset)
	if err != nil {
		return nil, err
	}
	s := &segment{
		baseOffset:         baseOffset,
		nextRelativeOffset: 0,
		nextPosition:       0,
		log:                log,
		localIndex:         index,
		logWriter:          logWriter,
		localIndexWriter:   indexWriter,
	}
	return s, nil
}

func newLog(partitionPath string, baseOffset int64) (*os.File, *bufio.Writer, error) {
	logName := getLogName(baseOffset)
	logPath := path.Join(partitionPath, logName)
	f, err := os.Create(logPath)
	if err != nil {
		return nil, nil, err
	}
	w := bufio.NewWriter(f)
	return f, w, nil
}

func newLocalIndex(partitionPath string, baseOffset int64) (*os.File, *bufio.Writer, error) {
	localIndexName := getLocalIndexName(baseOffset)
	localIndexPath := path.Join(partitionPath, localIndexName)
	f, err := os.Create(localIndexPath)
	if err != nil {
		return nil, nil, err
	}
	w := bufio.NewWriter(f)
	return f, w, nil
}

func newLogEntry(record string) string {
	l := &logEntry{
		Length: int32(len(record)),
		Record: record,
	}
	out, err := json.Marshal(l)
	if err != nil {
		log.Printf(err.Error())
	}
	return string(out)
}

func getGlobalIndexName(startGsn int64) string {
	return fmt.Sprintf("%019d%s", startGsn, globalIndexSuffix)
}

func getLogName(baseOffset int64) string {
	return fmt.Sprintf("%019d%s", baseOffset, logSuffix)
}

func getLocalIndexName(baseOffset int64) string {
	return fmt.Sprintf("%019d%s", baseOffset, localIndexSuffix)
}

func max(a, b int64) int64 {
	if a < b {
		return b
	}
	return a
}
