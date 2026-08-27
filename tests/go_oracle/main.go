// Go oracle for Rust↔Go bbolt cross-implementation tests.
//
// Speaks a small CLI used by `tests/cross_go_test.rs`. Requires network once
// (module download) and a Go toolchain; `GOTOOLCHAIN=auto` pulls the version
// required by go.etcd.io/bbolt.
package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	bolt "go.etcd.io/bbolt"
)

func main() {
	if len(os.Args) < 2 {
		fatalf("usage: go-oracle <init|write|mutate|inspect|assert|check|compact|writeto|dump-meta> ...")
	}
	var err error
	switch os.Args[1] {
	case "init":
		err = cmdInit(os.Args[2:])
	case "write":
		err = cmdWrite(os.Args[2:])
	case "mutate":
		err = cmdMutate(os.Args[2:])
	case "inspect":
		err = cmdInspect(os.Args[2:])
	case "assert":
		err = cmdAssert(os.Args[2:])
	case "check":
		err = cmdCheck(os.Args[2:])
	case "compact":
		err = cmdCompact(os.Args[2:])
	case "writeto":
		err = cmdWriteTo(os.Args[2:])
	case "dump-meta":
		err = cmdDumpMeta(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fatalf("%v", err)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "go-oracle: "+format+"\n", args...)
	os.Exit(1)
}

func openOpts(pageSize int, freelist string) *bolt.Options {
	o := *bolt.DefaultOptions
	if pageSize > 0 {
		o.PageSize = pageSize
	}
	switch freelist {
	case "", "array":
		o.FreelistType = bolt.FreelistArrayType
	case "hashmap", "map":
		o.FreelistType = bolt.FreelistMapType
	default:
		fatalf("unknown freelist type %q", freelist)
	}
	return &o
}

func openRO(path, freelist string) (*bolt.DB, error) {
	o := openOpts(0, freelist)
	o.ReadOnly = true
	return bolt.Open(path, 0o600, o)
}

func cmdInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	out := fs.String("o", "", "output db path")
	pageSize := fs.Int("pagesize", 4096, "page size")
	freelist := fs.String("freelist", "array", "array|hashmap")
	_ = fs.Parse(args)
	if *out == "" {
		return errors.New("-o required")
	}
	_ = os.Remove(*out)
	db, err := bolt.Open(*out, 0o600, openOpts(*pageSize, *freelist))
	if err != nil {
		return err
	}
	return db.Close()
}

func cmdWrite(args []string) error {
	fs := flag.NewFlagSet("write", flag.ExitOnError)
	out := fs.String("o", "", "output db path")
	scenario := fs.String("scenario", "sample", "scenario name")
	pageSize := fs.Int("pagesize", 4096, "page size")
	freelist := fs.String("freelist", "array", "array|hashmap")
	_ = fs.Parse(args)
	if *out == "" {
		return errors.New("-o required")
	}
	_ = os.Remove(*out)
	db, err := bolt.Open(*out, 0o600, openOpts(*pageSize, *freelist))
	if err != nil {
		return err
	}
	defer db.Close()
	return applyScenario(db, *scenario, true)
}

func cmdMutate(args []string) error {
	fs := flag.NewFlagSet("mutate", flag.ExitOnError)
	path := fs.String("db", "", "db path")
	scenario := fs.String("scenario", "", "mutation scenario")
	freelist := fs.String("freelist", "array", "array|hashmap")
	_ = fs.Parse(args)
	if *path == "" || *scenario == "" {
		return errors.New("-db and -scenario required")
	}
	db, err := bolt.Open(*path, 0o600, openOpts(0, *freelist))
	if err != nil {
		return err
	}
	defer db.Close()
	return applyScenario(db, *scenario, false)
}

func applyScenario(db *bolt.DB, name string, create bool) error {
	switch name {
	case "empty":
		return nil
	case "sample":
		return db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("users"))
			if err != nil {
				return err
			}
			if err := b.Put([]byte("alice"), []byte("data1")); err != nil {
				return err
			}
			if err := b.Put([]byte("bob"), []byte("data2")); err != nil {
				return err
			}
			nb, err := b.CreateBucketIfNotExists([]byte("nested"))
			if err != nil {
				return err
			}
			return nb.Put([]byte("x"), []byte("y"))
		})
	case "nested_deep":
		return db.Update(func(tx *bolt.Tx) error {
			a, err := tx.CreateBucketIfNotExists([]byte("a"))
			if err != nil {
				return err
			}
			b, err := a.CreateBucketIfNotExists([]byte("b"))
			if err != nil {
				return err
			}
			c, err := b.CreateBucketIfNotExists([]byte("c"))
			if err != nil {
				return err
			}
			if err := c.Put([]byte("leaf"), []byte("value")); err != nil {
				return err
			}
			return a.Put([]byte("sibling"), []byte("s"))
		})
	case "sequences":
		return db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("seq"))
			if err != nil {
				return err
			}
			if err := b.SetSequence(10); err != nil {
				return err
			}
			id, err := b.NextSequence()
			if err != nil {
				return err
			}
			if id != 11 {
				return fmt.Errorf("expected seq 11, got %d", id)
			}
			return b.Put([]byte("last"), []byte(fmt.Sprintf("%d", id)))
		})
	case "overflow":
		return db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("big"))
			if err != nil {
				return err
			}
			val := bytes.Repeat([]byte{0xAB}, 10_000)
			if err := b.Put([]byte("v"), val); err != nil {
				return err
			}
			return b.Put([]byte("small"), []byte("ok"))
		})
	case "split":
		return db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("split"))
			if err != nil {
				return err
			}
			for i := 0; i < 500; i++ {
				k := fmt.Sprintf("k%04d", i)
				v := fmt.Sprintf("v%04d-%s", i, strings.Repeat("x", 32))
				if err := b.Put([]byte(k), []byte(v)); err != nil {
					return err
				}
			}
			return nil
		})
	case "deletes":
		if err := db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("del"))
			if err != nil {
				return err
			}
			for i := 0; i < 200; i++ {
				k := fmt.Sprintf("%04d", i)
				if err := b.Put([]byte(k), []byte("x")); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		return db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("del"))
			for i := 0; i < 200; i += 2 {
				k := fmt.Sprintf("%04d", i)
				if err := b.Delete([]byte(k)); err != nil {
					return err
				}
			}
			return nil
		})
	case "multi_tx":
		for i := 0; i < 5; i++ {
			i := i
			if err := db.Update(func(tx *bolt.Tx) error {
				b, err := tx.CreateBucketIfNotExists([]byte("multi"))
				if err != nil {
					return err
				}
				return b.Put([]byte(fmt.Sprintf("t%d", i)), []byte(fmt.Sprintf("v%d", i)))
			}); err != nil {
				return err
			}
		}
		return nil
	case "mixed":
		for _, s := range []string{"sample", "sequences", "overflow", "split", "deletes", "nested_deep", "multi_tx"} {
			if err := applyScenario(db, s, create); err != nil {
				return fmt.Errorf("%s: %w", s, err)
			}
		}
		return nil
	case "add_keys":
		return db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("users"))
			if err != nil {
				return err
			}
			if err := b.Put([]byte("carol"), []byte("data3")); err != nil {
				return err
			}
			return b.Put([]byte("dave"), []byte("data4"))
		})
	case "delete_alice":
		return db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("users"))
			if b == nil {
				return errors.New("users missing")
			}
			return b.Delete([]byte("alice"))
		})
	case "bump_seq":
		return db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("seq"))
			if err != nil {
				return err
			}
			_, err = b.NextSequence()
			return err
		})
	case "incompatible_put":
		// Create a nested bucket then attempt Put on that key — expect error.
		return db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("widgets"))
			if err != nil {
				return err
			}
			if _, err := b.CreateBucketIfNotExists([]byte("sub")); err != nil {
				return err
			}
			err = b.Put([]byte("sub"), []byte("nope"))
			if !errors.Is(err, bolt.ErrIncompatibleValue) {
				return fmt.Errorf("expected ErrIncompatibleValue, got %v", err)
			}
			return nil
		})
	default:
		return fmt.Errorf("unknown scenario %q", name)
	}
}

type dumpBucket struct {
	Name     string       `json:"name"`
	Sequence uint64       `json:"sequence"`
	Keys     []dumpKV     `json:"keys"`
	Buckets  []dumpBucket `json:"buckets"`
}

type dumpKV struct {
	K string `json:"k"`
	V string `json:"v"`
}

type dumpDB struct {
	PageSize int          `json:"page_size"`
	Buckets  []dumpBucket `json:"buckets"`
}

func hx(b []byte) string {
	return hex.EncodeToString(b)
}

func dumpBucketTree(b *bolt.Bucket) (dumpBucket, error) {
	out := dumpBucket{
		Sequence: b.Sequence(),
		Keys:     make([]dumpKV, 0),
		Buckets:  make([]dumpBucket, 0),
	}
	var nested []dumpBucket
	err := b.ForEach(func(k, v []byte) error {
		if v == nil {
			sub := b.Bucket(k)
			if sub == nil {
				return fmt.Errorf("nil value but not a bucket for key %s", hx(k))
			}
			child, err := dumpBucketTree(sub)
			if err != nil {
				return err
			}
			child.Name = hx(k)
			nested = append(nested, child)
			return nil
		}
		out.Keys = append(out.Keys, dumpKV{K: hx(k), V: hx(v)})
		return nil
	})
	if err != nil {
		return out, err
	}
	sort.Slice(out.Keys, func(i, j int) bool { return out.Keys[i].K < out.Keys[j].K })
	sort.Slice(nested, func(i, j int) bool { return nested[i].Name < nested[j].Name })
	if nested == nil {
		nested = []dumpBucket{}
	}
	out.Buckets = nested
	return out, nil
}

func inspectDB(path, freelist string) (*dumpDB, error) {
	db, err := openRO(path, freelist)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	out := &dumpDB{PageSize: db.Info().PageSize, Buckets: []dumpBucket{}}
	err = db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			d, err := dumpBucketTree(b)
			if err != nil {
				return err
			}
			d.Name = hx(name)
			out.Buckets = append(out.Buckets, d)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out.Buckets, func(i, j int) bool { return out.Buckets[i].Name < out.Buckets[j].Name })
	return out, nil
}

func cmdInspect(args []string) error {
	fs := flag.NewFlagSet("inspect", flag.ExitOnError)
	path := fs.String("db", "", "db path")
	freelist := fs.String("freelist", "array", "array|hashmap")
	_ = fs.Parse(args)
	if *path == "" {
		return errors.New("-db required")
	}
	d, err := inspectDB(*path, *freelist)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(d)
}

func cmdAssert(args []string) error {
	fs := flag.NewFlagSet("assert", flag.ExitOnError)
	path := fs.String("db", "", "db path")
	expectPath := fs.String("expect", "", "expected JSON dump")
	freelist := fs.String("freelist", "array", "array|hashmap")
	_ = fs.Parse(args)
	if *path == "" || *expectPath == "" {
		return errors.New("-db and -expect required")
	}
	got, err := inspectDB(*path, *freelist)
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(*expectPath)
	if err != nil {
		return err
	}
	var want dumpDB
	if err := json.Unmarshal(raw, &want); err != nil {
		return err
	}
	gj, _ := json.Marshal(got)
	wj, _ := json.Marshal(&want)
	if !bytes.Equal(gj, wj) {
		return fmt.Errorf("inspect mismatch\n got: %s\nwant: %s", string(gj), string(wj))
	}
	fmt.Println("OK")
	return nil
}

func cmdCheck(args []string) error {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	path := fs.String("db", "", "db path")
	freelist := fs.String("freelist", "array", "array|hashmap")
	_ = fs.Parse(args)
	if *path == "" {
		return errors.New("-db required")
	}
	db, err := openRO(*path, *freelist)
	if err != nil {
		return err
	}
	defer db.Close()
	var errs []error
	err = db.View(func(tx *bolt.Tx) error {
		for e := range tx.Check() {
			errs = append(errs, e)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(errs) > 0 {
		for _, e := range errs {
			fmt.Fprintln(os.Stderr, e)
		}
		return fmt.Errorf("%d check errors", len(errs))
	}
	fmt.Println("OK")
	return nil
}

func cmdCompact(args []string) error {
	fs := flag.NewFlagSet("compact", flag.ExitOnError)
	src := fs.String("src", "", "source db")
	dst := fs.String("dst", "", "destination db")
	pageSize := fs.Int("pagesize", 4096, "dst page size")
	freelist := fs.String("freelist", "array", "array|hashmap")
	_ = fs.Parse(args)
	if *src == "" || *dst == "" {
		return errors.New("-src and -dst required")
	}
	_ = os.Remove(*dst)
	srcDB, err := openRO(*src, *freelist)
	if err != nil {
		return err
	}
	defer srcDB.Close()
	dstDB, err := bolt.Open(*dst, 0o600, openOpts(*pageSize, *freelist))
	if err != nil {
		return err
	}
	defer dstDB.Close()
	if err := bolt.Compact(dstDB, srcDB, 64*1024*1024); err != nil {
		return err
	}
	fmt.Println("OK")
	return nil
}

func cmdWriteTo(args []string) error {
	fs := flag.NewFlagSet("writeto", flag.ExitOnError)
	path := fs.String("db", "", "db path")
	out := fs.String("o", "", "output path")
	freelist := fs.String("freelist", "array", "array|hashmap")
	_ = fs.Parse(args)
	if *path == "" || *out == "" {
		return errors.New("-db and -o required")
	}
	db, err := openRO(*path, *freelist)
	if err != nil {
		return err
	}
	defer db.Close()
	f, err := os.Create(*out)
	if err != nil {
		return err
	}
	defer f.Close()
	return db.View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(f)
		return err
	})
}

func cmdDumpMeta(args []string) error {
	fs := flag.NewFlagSet("dump-meta", flag.ExitOnError)
	path := fs.String("db", "", "db path")
	_ = fs.Parse(args)
	if *path == "" {
		return errors.New("-db required")
	}
	raw, err := os.ReadFile(*path)
	if err != nil {
		return err
	}
	info := map[string]any{
		"size":     len(raw),
		"hex_head": hex.EncodeToString(raw[:min(256, len(raw))]),
	}
	if len(raw) >= 4096*2 {
		info["meta0"] = hex.EncodeToString(raw[16:80])
		info["meta1"] = hex.EncodeToString(raw[4096+16 : 4096+80])
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(info)
	abs, _ := filepath.Abs(*path)
	fmt.Fprintf(os.Stderr, "dumped %s (%d bytes)\n", abs, len(raw))
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Silence unused import if older compilers complain about io.
var _ = io.Discard
