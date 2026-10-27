package reader

import "github.com/hangxie/parquet-go/v3/internal/layout"

func (cbt *ColumnBufferType) retainPage(page *layout.Page) {
	if page.HasMoreValues() {
		cbt.pendingPage = page
	} else {
		cbt.pendingPage = nil
		cbt.finishIndexedDataPage()
	}
}

func (cbt *ColumnBufferType) dropSkippedContinuation(page *layout.Page) {
	if cbt.discardRowContinuation {
		cbt.discardRowContinuation = page.DropRowContinuation()
	}
}

func (cbt *ColumnBufferType) markSkippedRowTail() {
	if len(cbt.DataTable.Values) == 0 && cbt.DataTable.MaxRepetitionLevel > 0 {
		cbt.discardRowContinuation = true
	}
}

func (cbt *ColumnBufferType) skipPendingFlatRows(num int64) (int64, error) {
	if cbt.pendingPage == nil {
		return 0, nil
	}
	pending := cbt.pendingPage
	n, err := pending.SkipFlatRows(num)
	if err != nil {
		return 0, err
	}
	if n > 0 {
		cbt.ChunkReadValues += n
		cbt.retainPage(pending)
	}
	return n, nil
}
