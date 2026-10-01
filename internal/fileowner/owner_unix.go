//go:build unix

// Package fileowner reads a file's owning uid where the platform has one.
package fileowner

import (
	"os"
	"syscall"
)

// UID returns the file's owner uid; ok is false when it cannot be read.
func UID(fi os.FileInfo) (uid uint32, ok bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return st.Uid, true
}
