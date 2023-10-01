package sqlarfs_test

import (
	"syscall"
	"testing"
)

func TestFileMode(t *testing.T) {
	t.Logf("syscall.S_IFREG: %07o 0x%[1]x %10[1]d", syscall.S_IFREG)
	t.Logf("syscall.S_IFDIR: %07o 0x%[1]x %10[1]d", syscall.S_IFDIR)

	if syscall.S_IFREG != 0x8000 {
		t.Error("unexpected value for syscall.S_IFREG")
	}
	if syscall.S_IFDIR != 0x4000 {
		t.Error("unexpected value for syscall.S_IFDIR")
	}

	t.Logf("syscall.S_IFDIR | 0555: %07o 0x%[1]x %10[1]d", syscall.S_IFDIR|0555)
}
