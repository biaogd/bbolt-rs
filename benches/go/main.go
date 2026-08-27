// Apples-to-apples workload harness matching the Rust `bench_compare` binary.
//
// Timing covers only the measured phase (puts, gets, scan, deletes, etc.).
// Prepare/load steps for read and delete workloads are outside the timer.
//
// Output line:
//   impl=go workload=... n=... elapsed_ns=... ops_sec=... filesize=... nosync=bool key_size=... value_size=...
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

func main() {
	dir := flag.String("dir", "", "directory for db file (required)")
	workload := flag.String("workload", "", "workload name (required)")
	n := flag.Int("n", 100000, "number of keys / ops")
	keySize := flag.Int("key-size", 8, "key size in bytes (>=8)")
	valueSize := flag.Int("value-size", 32, "value size in bytes")
	pageSize := flag.Int("page-size", 4096, "page size")
	noSync := flag.Bool("no-sync", false, "disable fsync (NoSync)")
	deleteFrac := flag.Float64("delete-frac", 0.5, "fraction of keys to delete (deletes)")
	batchSize := flag.Int("batch-size", 1, "keys per Update for many_small_tx")
	seed := flag.Int64("seed", 1, "RNG seed")
	flag.Parse()
	if *dir == "" || *workload == "" {
		fatalf("-dir and -workload required")
	}
	if *keySize < 8 {
		fatalf("key-size must be >= 8")
	}
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		fatalf("%v", err)
	}
	path := filepath.Join(*dir, "bench.db")
	_ = os.Remove(path)

	opts := *bolt.DefaultOptions
	opts.PageSize = *pageSize
	opts.FreelistType = bolt.FreelistArrayType
	opts.NoSync = *noSync
	opts.NoGrowSync = false

	rng := rand.New(rand.NewSource(*seed))
	var (
		elapsed time.Duration
		ops     int
		err     error
	)
	switch *workload {
	case "seq_put", "one_large_tx", "large_value":
		ops, elapsed, err = timedSeqPut(path, &opts, *n, *keySize, *valueSize)
	case "random_put":
		ops, elapsed, err = timedRandomPut(path, &opts, *n, *keySize, *valueSize, rng)
	case "random_get":
		if err = prepareSeqPut(path, &opts, *n, *keySize, *valueSize); err != nil {
			fatalf("prepare: %v", err)
		}
		ops, elapsed, err = timedRandomGet(path, &opts, *n, *keySize, rng)
	case "cursor_scan":
		if err = prepareSeqPut(path, &opts, *n, *keySize, *valueSize); err != nil {
			fatalf("prepare: %v", err)
		}
		ops, elapsed, err = timedCursorScan(path, &opts)
	case "deletes":
		if err = prepareSeqPut(path, &opts, *n, *keySize, *valueSize); err != nil {
			fatalf("prepare: %v", err)
		}
		ops, elapsed, err = timedDeletes(path, &opts, *n, *keySize, *deleteFrac)
	case "many_small_tx":
		ops, elapsed, err = timedManySmallTx(path, &opts, *n, *keySize, *valueSize, *batchSize)
	default:
		fatalf("unknown workload %q", *workload)
	}
	if err != nil {
		fatalf("%v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		fatalf("%v", err)
	}
	opsSec := float64(ops) / elapsed.Seconds()
	fmt.Printf(
		"impl=go workload=%s n=%d elapsed_ns=%d ops_sec=%.2f filesize=%d nosync=%t key_size=%d value_size=%d\n",
		*workload, ops, elapsed.Nanoseconds(), opsSec, fi.Size(), *noSync, *keySize, *valueSize,
	)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "go-bench: "+format+"\n", args...)
	os.Exit(1)
}

func openOpts(opts *bolt.Options) *bolt.Options {
	o := *opts
	return &o
}

func makeKey(buf []byte, i int) {
	for j := range buf {
		buf[j] = 0
	}
	binary.BigEndian.PutUint64(buf[len(buf)-8:], uint64(i))
}

func makeVal(buf []byte, i int) {
	for j := range buf {
		buf[j] = byte(i + j)
	}
}

func prepareSeqPut(path string, opts *bolt.Options, n, keySize, valueSize int) error {
	_ = os.Remove(path)
	_, _, err := timedSeqPut(path, opts, n, keySize, valueSize)
	return err
}

func timedSeqPut(path string, opts *bolt.Options, n, keySize, valueSize int) (int, time.Duration, error) {
	db, err := bolt.Open(path, 0o600, openOpts(opts))
	if err != nil {
		return 0, 0, err
	}
	key := make([]byte, keySize)
	val := make([]byte, valueSize)
	start := time.Now()
	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("bench"))
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			makeKey(key, i)
			makeVal(val, i)
			if err := b.Put(key, val); err != nil {
				return err
			}
		}
		return nil
	})
	elapsed := time.Since(start)
	cerr := db.Close()
	if err != nil {
		return 0, elapsed, err
	}
	return n, elapsed, cerr
}

func timedRandomPut(path string, opts *bolt.Options, n, keySize, valueSize int, rng *rand.Rand) (int, time.Duration, error) {
	db, err := bolt.Open(path, 0o600, openOpts(opts))
	if err != nil {
		return 0, 0, err
	}
	order := rng.Perm(n)
	key := make([]byte, keySize)
	val := make([]byte, valueSize)
	start := time.Now()
	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("bench"))
		if err != nil {
			return err
		}
		for _, i := range order {
			makeKey(key, i)
			makeVal(val, i)
			if err := b.Put(key, val); err != nil {
				return err
			}
		}
		return nil
	})
	elapsed := time.Since(start)
	cerr := db.Close()
	if err != nil {
		return 0, elapsed, err
	}
	return n, elapsed, cerr
}

func timedRandomGet(path string, opts *bolt.Options, n, keySize int, rng *rand.Rand) (int, time.Duration, error) {
	db, err := bolt.Open(path, 0o600, openOpts(opts))
	if err != nil {
		return 0, 0, err
	}
	defer db.Close()
	order := rng.Perm(n)
	key := make([]byte, keySize)
	start := time.Now()
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("bench"))
		if b == nil {
			return fmt.Errorf("missing bench bucket")
		}
		for _, i := range order {
			makeKey(key, i)
			if b.Get(key) == nil {
				return fmt.Errorf("missing key %d", i)
			}
		}
		return nil
	})
	return n, time.Since(start), err
}

func timedCursorScan(path string, opts *bolt.Options) (int, time.Duration, error) {
	db, err := bolt.Open(path, 0o600, openOpts(opts))
	if err != nil {
		return 0, 0, err
	}
	defer db.Close()
	var count int
	start := time.Now()
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("bench"))
		if b == nil {
			return fmt.Errorf("missing bench bucket")
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			count++
		}
		return nil
	})
	return count, time.Since(start), err
}

func timedDeletes(path string, opts *bolt.Options, n, keySize int, frac float64) (int, time.Duration, error) {
	db, err := bolt.Open(path, 0o600, openOpts(opts))
	if err != nil {
		return 0, 0, err
	}
	delN := int(float64(n) * frac)
	key := make([]byte, keySize)
	start := time.Now()
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("bench"))
		if b == nil {
			return fmt.Errorf("missing bench bucket")
		}
		for i := 0; i < delN; i++ {
			makeKey(key, i)
			if err := b.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
	elapsed := time.Since(start)
	cerr := db.Close()
	if err != nil {
		return 0, elapsed, err
	}
	return delN, elapsed, cerr
}

func timedManySmallTx(path string, opts *bolt.Options, n, keySize, valueSize, batchSize int) (int, time.Duration, error) {
	db, err := bolt.Open(path, 0o600, openOpts(opts))
	if err != nil {
		return 0, 0, err
	}
	if batchSize < 1 {
		batchSize = 1
	}
	key := make([]byte, keySize)
	val := make([]byte, valueSize)
	start := time.Now()
	for i := 0; i < n; {
		end := i + batchSize
		if end > n {
			end = n
		}
		lo := i
		hi := end
		err = db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("bench"))
			if err != nil {
				return err
			}
			for j := lo; j < hi; j++ {
				makeKey(key, j)
				makeVal(val, j)
				if err := b.Put(key, val); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			_ = db.Close()
			return 0, time.Since(start), err
		}
		i = end
	}
	elapsed := time.Since(start)
	return n, elapsed, db.Close()
}
