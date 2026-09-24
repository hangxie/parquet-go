package reader

import (
	"context"
	"errors"
	"fmt"

	"github.com/hangxie/parquet-go/v3/source"
)

// A column chunk may name the file its pages live in through ColumnChunk.file_path,
// leaving the metadata file holding nothing but the footer for that chunk. The buffer
// therefore keeps two handles: mainFile, the file the footer was read from, and an
// external handle for the chunk being read, if it names one. PFile is whichever of the
// two the current chunk reads from, so every existing reader keeps working unchanged.

// metadataFile is the handle external paths are resolved against and the one a chunk
// naming no file is read from.
func (cbt *ColumnBufferType) metadataFile() source.ParquetFileReader {
	if cbt.mainFile != nil {
		return cbt.mainFile
	}
	// A buffer assembled without going through newColumnBuffer has only PFile.
	return cbt.PFile
}

// selectChunkFile points the buffer at the file a column chunk's pages live in, opening
// the file it names or returning to the metadata file when it names none.
func (cbt *ColumnBufferType) selectChunkFile(filePath *string) error {
	if filePath == nil || *filePath == "" {
		cbt.releaseExternalFile()
		cbt.setChunkFile(cbt.metadataFile())
		return nil
	}
	if cbt.externalFile != nil && cbt.externalPath == *filePath {
		cbt.setChunkFile(cbt.externalFile)
		return nil
	}
	// Open before releasing the handle it replaces: a failed Open returns a nil interface,
	// and the buffer must be left with a usable handle either way. The path is resolved
	// against the metadata file, which is what the format measures it from, rather than
	// against whichever external file happened to be open.
	pFile, err := source.OpenWithContext(cbt.context(), cbt.metadataFile(), *filePath)
	if err != nil {
		return fmt.Errorf("open file %s: %w", *filePath, err)
	}
	cbt.releaseExternalFile()
	cbt.externalFile, cbt.externalPath = pFile, *filePath
	cbt.setChunkFile(pFile)
	return nil
}

// setChunkFile makes file the handle the current chunk reads from, releasing the handle it
// replaces when that was a transient clone rather than one of the two the buffer owns.
func (cbt *ColumnBufferType) setChunkFile(file source.ParquetFileReader) {
	old := cbt.PFile
	cbt.PFile = file
	if old == nil || old == file || old == cbt.mainFile || old == cbt.externalFile {
		return
	}
	_ = source.CloseWithContext(cbt.context(), old)
}

// releaseExternalFile closes the external handle, if the buffer holds one.
func (cbt *ColumnBufferType) releaseExternalFile() {
	if cbt.externalFile == nil {
		return
	}
	if cbt.PFile == cbt.externalFile {
		cbt.PFile = cbt.metadataFile()
	}
	_ = source.CloseWithContext(cbt.context(), cbt.externalFile)
	cbt.externalFile, cbt.externalPath = nil, ""
}

// closeFiles releases every handle the buffer owns: the metadata file, any external file,
// and any transient clone left in place by a seek through the offset index. It is safe to
// call more than once, so a reader that is reset and then stopped closes each handle once.
func (cbt *ColumnBufferType) closeFiles(ctx context.Context) error {
	var errs []error
	if cbt.PFile != nil && cbt.PFile != cbt.mainFile && cbt.PFile != cbt.externalFile {
		errs = append(errs, source.CloseWithContext(ctx, cbt.PFile))
	}
	if cbt.externalFile != nil {
		errs = append(errs, source.CloseWithContext(ctx, cbt.externalFile))
	}
	if cbt.mainFile != nil {
		errs = append(errs, source.CloseWithContext(ctx, cbt.mainFile))
	}
	cbt.PFile, cbt.mainFile, cbt.externalFile = nil, nil, nil
	cbt.externalPath = ""
	return errors.Join(errs...)
}
