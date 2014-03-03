package main

import (
	"github.com/calmh/syncthing/files"
	"github.com/calmh/syncthing/protocol"
)

func fsFilesFromFiles(fs []File) []files.File {
	var ffs = make([]files.File, len(fs))
	for i := range ffs {
		ffs[i] = fsFileFromFile(fs[i])
	}
	return ffs
}

func fsFileFromFile(f File) files.File {
	return files.File{
		Key: files.Key{
			Name:    f.Name,
			Version: f.Version,
		},
		Data: f,
	}
}

func filesFromFileInfos(fs []protocol.FileInfo) []File {
	var ffs = make([]File, len(fs))
	for i := range ffs {
		ffs[i] = fileFromFileInfo(fs[i])
	}
	return ffs
}

func fileFromFileInfo(f protocol.FileInfo) File {
	var blocks = make([]Block, len(f.Blocks))
	var offset int64
	for i, b := range f.Blocks {
		blocks[i] = Block{
			Offset: offset,
			Size:   b.Size,
			Hash:   b.Hash,
		}
		offset += int64(b.Size)
	}
	return File{
		Name:     f.Name,
		Size:     offset,
		Flags:    f.Flags,
		Modified: f.Modified,
		Version:  f.Version,
		Blocks:   blocks,
	}
}

func fileInfoFromFile(f File) protocol.FileInfo {
	var blocks = make([]protocol.BlockInfo, len(f.Blocks))
	for i, b := range f.Blocks {
		blocks[i] = protocol.BlockInfo{
			Size: b.Size,
			Hash: b.Hash,
		}
	}
	return protocol.FileInfo{
		Name:     f.Name,
		Flags:    f.Flags,
		Modified: f.Modified,
		Version:  f.Version,
		Blocks:   blocks,
	}
}
