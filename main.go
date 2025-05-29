package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/parser"
	log "github.com/sirupsen/logrus"
)

var rdbFileName string
var targetSizeGB int
var logLevel string
var logger = log.StandardLogger()

func initLogger() {
	if logLevel == "debug" {
		logger.SetLevel(log.DebugLevel)
	} else if logLevel == "trace" {
		logger.SetLevel(log.TraceLevel)
	} else {
		logger.SetLevel(log.InfoLevel)
	}

	logger.SetFormatter(&log.TextFormatter{
		FullTimestamp: false,
	})
}

const rdbDirName = "rdb_dir"

func main() {
	flag.StringVar(&rdbFileName, "name", "dump.rdb", "rdb file name")
	flag.IntVar(&targetSizeGB, "target-size-gb", 4, "target size in GB")
	flag.StringVar(&logLevel, "log", "info", "log level: info/debug/trace")
	flag.Parse()

	initLogger()

	logger.Warnf("start split rdb file %s, target size (GB) %d\n", rdbFileName, targetSizeGB)
	if targetSizeGB <= 0 {
		logger.Fatalf("target size in GB must be > 0")
	}

	// Open source RDB file
	srcFile, err := os.Open(rdbFileName)
	if err != nil {
		logger.Fatalf("open rdb file %s error: %s\n", rdbFileName, err)
	}
	defer srcFile.Close()

	// Get file size
	stat, err := srcFile.Stat()
	if err != nil {
		logger.Fatalf("stat rdb file %s error: %s\n", rdbFileName, err)
	}
	totalSize := stat.Size()

	targetSize := int64(targetSizeGB) * 1024 * 1024 * 1024
	if targetSize >= totalSize/2 {
		logger.Fatalf("target size (GB) must be <= half of total size %d (GB)", totalSize/1024/1024/1024)
	}

	// Dist dir create
	if err = os.Mkdir(rdbDirName, os.ModePerm); err != nil {
		// ignore file exists
		if !os.IsExist(err) {
			logger.Fatalf("create dir %s error: %s\n", rdbDirName, err)
		}
	}

	// Prepare to parse
	decoder := parser.NewDecoder(srcFile)

	// For output
	var (
		curIdx                      int   = 1
		curSize                     int64 = 0
		curFile                     *os.File
		curEnc                      *encoder.Encoder
		resetKeyCountFileBeginIndex int64
		lastSetDBIndex              int = -1
		keyCount                    int
		ttlCount                    int
	)
	openNewSubRDB := func() {
		if curFile != nil {
			curEnc.WriteEnd()
			if resetKeyCountFileBeginIndex != 0 {
				// todo, rewrite key count and ttl count
			}
			curFile.Close()

			resetKeyCountFileBeginIndex = 0
			keyCount = 0
			ttlCount = 0
			lastSetDBIndex = -1
		}
		outName := fmt.Sprintf("part%d.rdb", curIdx)
		file, err := os.Create(rdbDirName + "/" + outName)
		if err != nil {
			logger.Fatalf("create sub rdb file %s error: %s\n", outName, err)
		}
		enc := encoder.NewEncoder(file)
		if err := enc.WriteHeader(); err != nil {
			logger.Fatalf("write header to %s error: %s\n", outName, err)
		}
		// Optionally write AUX fields (optional, can be omitted)
		auxMap := map[string]string{
			"redis-ver":    "6.2.6",
			"redis-bits":   "64",
			"aof-preamble": "0",
		}
		for k, v := range auxMap {
			_ = enc.WriteAux(k, v)
		}
		curFile = file
		curEnc = enc
		curSize = 0
		logger.Infof("Opened new sub RDB: %s", outName)
	}

	openNewSubRDB()

	// Parse and split
	err = decoder.Parse(func(obj parser.RedisObject) bool {
		objSize := int64(obj.GetSize())
		if curSize+objSize > targetSize && curSize > 0 {
			curIdx++
			openNewSubRDB()
		}
		if lastSetDBIndex != obj.GetDBIndex() {
			// tmp write key count, ttl count 0, mark current file length, then rewrite after this file write done
			curEnc.WriteDBHeader(uint(obj.GetDBIndex()), 0, 0)
			lastSetDBIndex = obj.GetDBIndex()

			if resetKeyCountFileBeginIndex == 0 {
				stat, _ := curFile.Stat()
				resetKeyCountFileBeginIndex = stat.Size()
			}
		}

		// Write to current encoder
		switch o := obj.(type) {
		case *parser.StringObject:
			keyCount++
			if o.Expiration != nil {
				curEnc.WriteStringObject(o.Key, o.Value, encoder.WithTTL(uint64(o.Expiration.UnixNano()/1e6)))
				ttlCount++
			} else {
				curEnc.WriteStringObject(o.Key, o.Value)
			}
		case *parser.ListObject:
			keyCount++
			if o.Expiration != nil {
				curEnc.WriteListObject(o.Key, o.Values, encoder.WithTTL(uint64(o.Expiration.UnixNano()/1e6)))
				ttlCount++
			} else {
				curEnc.WriteListObject(o.Key, o.Values)
			}
		case *parser.HashObject:
			keyCount++
			if o.Expiration != nil {
				curEnc.WriteHashMapObject(o.Key, o.Hash, encoder.WithTTL(uint64(o.Expiration.UnixNano()/1e6)))
				ttlCount++
			} else {
				curEnc.WriteHashMapObject(o.Key, o.Hash)
			}
		case *parser.SetObject:
			keyCount++
			if o.Expiration != nil {
				curEnc.WriteSetObject(o.Key, o.Members, encoder.WithTTL(uint64(o.Expiration.UnixNano()/1e6)))
				ttlCount++
			} else {
				curEnc.WriteSetObject(o.Key, o.Members)
			}
		case *parser.ZSetObject:
			keyCount++
			if o.Expiration != nil {
				curEnc.WriteZSetObject(o.Key, o.Entries, encoder.WithTTL(uint64(o.Expiration.UnixNano()/1e6)))
				ttlCount++
			} else {
				curEnc.WriteZSetObject(o.Key, o.Entries)
			}
		case *parser.StreamObject:
			// Do nothing
		}
		curSize += objSize
		return true
	})
	if err != nil {
		logger.Fatalf("parse rdb file error: %v", err)
	}
	// Finalize last file
	if curFile != nil {
		curEnc.WriteEnd()
		// key count / ttl count rewrite, todo
		curFile.Close()
	}
	logger.Infof("Done splitting RDB file into %d parts.", curIdx)
}
