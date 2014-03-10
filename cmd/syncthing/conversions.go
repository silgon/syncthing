package main

import (
	"github.com/calmh/syncthing/files"
	"github.com/calmh/syncthing/protocol"
	"github.com/calmh/syncthing/scanner"
)

func fsFilesFromFiles(fs []scanner.File) []files.File {
	var ffs = make([]files.File, len(fs))
	for i := range ffs {
		ffs[i] = fsFileFromFile(fs[i])
	}
	return ffs
}

func fsFileFromFile(f scanner.File) files.File {
	return files.File{
		Key: files.Key{
			Name:    f.Name,
			Version: f.Version,
		},
		Data: f,
	}
}

func filesFromFileInfos(fs []protocol.FileInfo) []scanner.File {
	var ffs = make([]scanner.File, len(fs))
	for i := range ffs {
		ffs[i] = fileFromFileInfo(fs[i])
	}
	return ffs
}

func fileFromFileInfo(f protocol.FileInfo) scanner.File {
	var blocks = make([]scanner.Block, len(f.Blocks))
	var offset int64
	for i, b := range f.Blocks {
		blocks[i] = scanner.Block{
			Offset: offset,
			Size:   b.Size,
			Hash:   b.Hash,
		}
		offset += int64(b.Size)
	}
	return scanner.File{
		Name:     f.Name,
		Size:     offset,
		Flags:    f.Flags,
		Modified: f.Modified,
		Version:  f.Version,
		Blocks:   blocks,
	}
}

func fileInfoFromFile(f scanner.File) protocol.FileInfo {
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

func fileInfosFromFiles(fs []scanner.File) []protocol.FileInfo {
	var fis = make([]protocol.FileInfo, len(fs))
	for i := range fs {
		fis[i] = fileInfoFromFile(fs[i])
	}
	return fis

}
