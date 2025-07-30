package ouroboroskv

import (
	"errors"
	"os"
	"syscall"

	"github.com/sirupsen/logrus"
)

type StoreConfig struct {
	Paths            []string // absolute path at the moment only first path is supported
	MinimumFreeSpace int      // in GB
	Logger           *logrus.Logger
}

func (sc *StoreConfig) checkConfig() error {
	if len(sc.Paths) == 0 {
		return errors.New("no path provided in configuration")
	}

	// create the directory if it does not exist
	if _, err := os.Stat(sc.Paths[0]); os.IsNotExist(err) {
		err := os.MkdirAll(sc.Paths[0], 0755)
		if err != nil {
			return errors.New("could not create directory")
		}
	}

	path := sc.Paths[0] // Currently only the first path is utilized
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return errors.New("path does not exist")
	}
	if !info.IsDir() {
		return errors.New("path is not a directory")
	}

	var stat syscall.Statfs_t
	syscall.Statfs(path, &stat)

	// Available blocks * size per block gives available space in bytes
	availableSpaceInGB := (stat.Bavail * uint64(stat.Bsize)) / (1024 * 1024 * 1024)
	if int(availableSpaceInGB) < sc.MinimumFreeSpace {
		return errors.New("not enough space available on disk")
	}

	return nil
}
