package db

import (
	"encoding/binary"
	"fmt"

	dbm "github.com/cometbft/cometbft-db"
	cmterrors "github.com/cometbft/cometbft/types/errors"
	"github.com/google/orderedcode"

	cmtproto "github.com/cometbft/cometbft/api/cometbft/types/v1"
	cmtsync "github.com/cometbft/cometbft/internal/sync"
	"github.com/cometbft/cometbft/light/store"
	"github.com/cometbft/cometbft/types"
)

const (
	// subkeys must be unique within a single DB.
	subkeyLightBlock = int64(9)
	subkeySize       = int64(10)
)

type dbs struct {
	db     dbm.DB
	prefix string

	mtx  cmtsync.RWMutex
	size uint16
}

// New returns a Store that wraps any DB (with an optional prefix in case you
// want to use one DB with many light clients).
func New(db dbm.DB, prefix string) store.Store {
	lightStore := &dbs{db: db, prefix: prefix}
	// retrieve the size of the db
	size := uint16(0)
	bz, err := lightStore.db.Get(lightStore.sizeKey())
	if err == nil && len(bz) > 0 {
		size = unmarshalSize(bz)
	}
	lightStore.size = size

	return lightStore
}

// SaveLightBlock persists LightBlock to the db.
//
// Safe for concurrent use by multiple goroutines.
func (s *dbs) SaveLightBlock(lb *types.LightBlock) error {
	if lb.Height <= 0 {
		panic("negative or zero height")
	}

	lbpb, err := lb.ToProto()
	if err != nil {
		return cmterrors.ErrMsgToProto{MessageName: "LightBlock", Err: err}
	}

	lbBz, err := lbpb.Marshal()
	if err != nil {
		return fmt.Errorf("marshaling LightBlock: %w", err)
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	b := s.db.NewBatch()
	defer b.Close()
	if err = b.Set(s.lbKey(lb.Height), lbBz); err != nil {
		return err
	}
	if err = b.Set(s.sizeKey(), marshalSize(s.size+1)); err != nil {
		return err
	}
	if err = b.WriteSync(); err != nil {
		return err
	}
	s.size++

	return nil
}

// DeleteLightBlockAndValidatorSet deletes the LightBlock from
// the db.
//
// Safe for concurrent use by multiple goroutines.
func (s *dbs) DeleteLightBlock(height int64) error {
	if height <= 0 {
		panic("negative or zero height")
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Delete(s.lbKey(height)); err != nil {
		return err
	}
	if err := b.Set(s.sizeKey(), marshalSize(s.size-1)); err != nil {
		return err
	}
	if err := b.WriteSync(); err != nil {
		return err
	}
	s.size--

	return nil
}

// LightBlock retrieves the LightBlock at the given height.
//
// Safe for concurrent use by multiple goroutines.
func (s *dbs) LightBlock(height int64) (*types.LightBlock, error) {
	if height <= 0 {
		panic("negative or zero height")
	}

	bz, err := s.db.Get(s.lbKey(height))
	if err != nil {
		panic(err)
	}
	if len(bz) == 0 {
		return nil, store.ErrLightBlockNotFound
	}

	var lbpb cmtproto.LightBlock
	err = lbpb.Unmarshal(bz)
	if err != nil {
		return nil, fmt.Errorf("unmarshal error: %w", err)
	}

	lightBlock, err := types.LightBlockFromProto(&lbpb)
	if err != nil {
		return nil, fmt.Errorf("proto conversion error: %w", err)
	}

	return lightBlock, err
}

// LastLightBlockHeight returns the last LightBlock height stored.
//
// Safe for concurrent use by multiple goroutines.
func (s *dbs) LastLightBlockHeight() (int64, error) {
	itr, err := s.db.ReverseIterator(
		s.lbKey(1),
		append(s.lbKey(1<<63-1), byte(0x00)),
	)
	if err != nil {
		panic(err)
	}
	defer itr.Close()

	if itr.Valid() {
		return s.decodeLbKey(itr.Key())
	}

	return -1, itr.Error()
}

// FirstLightBlockHeight returns the first LightBlock height stored.
//
// Safe for concurrent use by multiple goroutines.
func (s *dbs) FirstLightBlockHeight() (int64, error) {
	itr, err := s.db.Iterator(
		s.lbKey(1),
		append(s.lbKey(1<<63-1), byte(0x00)),
	)
	if err != nil {
		panic(err)
	}
	defer itr.Close()

	if itr.Valid() {
		return s.decodeLbKey(itr.Key())
	}

	return -1, itr.Error()
}

// LightBlockBefore iterates over light blocks until it finds a block before
// the given height. It returns ErrLightBlockNotFound if no such block exists.
//
// Safe for concurrent use by multiple goroutines.
func (s *dbs) LightBlockBefore(height int64) (*types.LightBlock, error) {
	if height <= 0 {
		panic("negative or zero height")
	}

	itr, err := s.db.ReverseIterator(
		s.lbKey(1),
		s.lbKey(height),
	)
	if err != nil {
		panic(err)
	}
	defer itr.Close()

	if itr.Valid() {
		existingHeight, err := s.decodeLbKey(itr.Key())
		if err != nil {
			return nil, err
		}
		return s.LightBlock(existingHeight)
	}
	if err = itr.Error(); err != nil {
		return nil, err
	}

	return nil, store.ErrLightBlockNotFound
}

// Prune prunes header & validator set pairs until there are only size pairs
// left.
//
// Safe for concurrent use by multiple goroutines.
func (s *dbs) Prune(size uint16) error {
	// 1) Check how many we need to prune.
	s.mtx.RLock()
	sSize := s.size
	s.mtx.RUnlock()

	if sSize <= size { // nothing to prune
		return nil
	}
	numToPrune := sSize - size

	// 2) Iterate over headers and perform a batch operation.
	itr, err := s.db.Iterator(
		s.lbKey(1),
		append(s.lbKey(1<<63-1), byte(0x00)),
	)
	if err != nil {
		return err
	}
	defer itr.Close()

	b := s.db.NewBatch()
	defer b.Close()

	pruned := 0
	for itr.Valid() && numToPrune > 0 {
		key := itr.Key()
		height, err := s.decodeLbKey(key)
		if err != nil {
			return err
		}
		if err = b.Delete(s.lbKey(height)); err != nil {
			return err
		}
		itr.Next()
		numToPrune--
		pruned++
	}
	if err = itr.Error(); err != nil {
		return err
	}

	err = b.WriteSync()
	if err != nil {
		return err
	}

	// 3) Update size.
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.size -= uint16(pruned)

	if wErr := s.db.SetSync(s.sizeKey(), marshalSize(s.size)); wErr != nil {
		return fmt.Errorf("failed to persist size: %w", wErr)
	}

	return nil
}

// Size returns the number of header & validator set pairs.
//
// Safe for concurrent use by multiple goroutines.
func (s *dbs) Size() uint16 {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.size
}

func (s *dbs) sizeKey() []byte {
	key, err := orderedcode.Append(nil, s.prefix, subkeySize)
	if err != nil {
		panic(err)
	}
	return key
}

func (s *dbs) lbKey(height int64) []byte {
	key, err := orderedcode.Append(nil, s.prefix, subkeyLightBlock, height)
	if err != nil {
		panic(err)
	}
	return key
}

func (s *dbs) decodeLbKey(key []byte) (height int64, err error) {
	var (
		dbPrefix string
		subkey   int64
	)
	remaining, err := orderedcode.Parse(string(key), &dbPrefix, &subkey, &height)
	if err != nil {
		err = fmt.Errorf("failed to parse light block key: %w", err)
	}
	if len(remaining) != 0 {
		err = fmt.Errorf("expected no remainder when parsing light block key but got: %s", remaining)
	}
	if subkey != subkeyLightBlock {
		err = fmt.Errorf("expected light block subkey but got: %d", subkey)
	}
	if dbPrefix != s.prefix {
		err = fmt.Errorf("parsed key has a different prefix. Expected: %s, got: %s", s.prefix, dbPrefix)
	}
	return
}

func marshalSize(size uint16) []byte {
	bs := make([]byte, 2)
	binary.LittleEndian.PutUint16(bs, size)
	return bs
}

func unmarshalSize(bz []byte) uint16 {
	return binary.LittleEndian.Uint16(bz)
}
