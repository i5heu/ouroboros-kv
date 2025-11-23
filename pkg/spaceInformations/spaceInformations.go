package spaceInformations

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/shirou/gopsutil/disk"
)

// getDiskUsageStats gets the disk usage statistics of the given path
func getDiskUsageStats(path string) (disk syscall.Statfs_t, err error) {
	err = syscall.Statfs(path, &disk)
	return
}

// calculateDirectorySize calculates the total size of files within a directory
func CalculateDirectorySize(path string) (size int64, err error) {
	err = filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return
}

func GetDeviceAndMountPoint(path string) (string, string, error) {
	partitions, err := disk.Partitions(true)
	if err != nil {
		return "", "", err
	}

	slog.Debug("Partitions found", "partitions", partitions)

	// Find the absolute path
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", "", err
	}

	matchPath := absPath
	foundExisting := false
	current := absPath
	for {
		resolved, err := filepath.EvalSymlinks(current)
		if err == nil {
			current = resolved
		}

		_, infoErr := os.Stat(current)
		if infoErr == nil {
			matchPath = current
			foundExisting = true
			break
		}

		if !os.IsNotExist(infoErr) {
			return "", "", infoErr
		}

		parent := filepath.Dir(current)
		if parent == current {
			break
		}
		current = parent
	}

	if !foundExisting {
		return "", "", fmt.Errorf("path does not exist: %s", path)
	}

	if matchPath == string(os.PathSeparator) && matchPath != absPath {
		return "", "", fmt.Errorf("path does not exist beyond root: %s", path)
	}

	for _, partition := range partitions {
		if contains(matchPath, partition.Mountpoint) {
			return partition.Mountpoint, partition.Device, nil
		}
	}

	return "", "", fmt.Errorf("mount point not found for path: %s", path)
}

// contains checks if a path is within the mount point.
func contains(path, mountpoint string) bool {
	if mountpoint == "" {
		return false
	}

	p := filepath.Clean(path)
	m := filepath.Clean(mountpoint)

	if m == string(os.PathSeparator) {
		return true
	}

	if p == m {
		return true
	}

	if strings.HasSuffix(m, string(os.PathSeparator)) {
		m = strings.TrimSuffix(m, string(os.PathSeparator))
	}

	return strings.HasPrefix(p, m+string(os.PathSeparator))
}

// DisplayDiskUsage displays the disk usage information using structured logging
func DisplayDiskUsage(paths []string) error {

	if len(paths) == 0 {
		slog.Error("No path provided in configuration")
		return fmt.Errorf("no path provided in configuration")
	}

	if paths[0] == "ExamplePath" {
		return nil

	}

	for _, path := range paths {
		disk, err := getDiskUsageStats(path)
		if err != nil {
			slog.Error("Error retrieving disk usage stats", "path", path, "error", err)
			return err
		}

		mountPoint, device, err := GetDeviceAndMountPoint(path)
		if err != nil {
			slog.Error("Error finding device and mount point", "path", path, "error", err)
			return err
		}

		totalSpace := float64(disk.Blocks*uint64(disk.Bsize)) / 1e9
		freeSpace := float64(disk.Bfree*uint64(disk.Bsize)) / 1e9
		usedSpace := totalSpace - freeSpace

		pathSize, err := CalculateDirectorySize(path)
		if err != nil {
			slog.Error("Error calculating directory size", "path", path, "error", err)
			return err
		}
		pathUsage := float64(pathSize) / 1e9

		slog.Info("Disk Usage information for path",
			"Path", path,
			"Device", device,
			"Mount Point", mountPoint,
			"Total (GB)", fmt.Sprintf("%.2f", totalSpace),
			"Used (GB)", fmt.Sprintf("%.2f", usedSpace),
			"Free (GB)", fmt.Sprintf("%.2f", freeSpace),
			"Usage by DB", fmt.Sprintf("%.2f", pathUsage),
		)
	}

	return nil
}
