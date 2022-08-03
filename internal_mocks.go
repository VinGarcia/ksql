package ksql

import "context"

// mockTxBeginner mocks the ksql.TxBeginner interface
type mockTxBeginner struct {
	DBAdapter
	BeginTxFn func(ctx context.Context) (Tx, error)
}

func (b mockTxBeginner) BeginTx(ctx context.Context) (Tx, error) {
	return b.BeginTxFn(ctx)
}

// mockTx mocks the ksql.Tx interface
type mockTx struct {
	DBAdapter
	RollbackFn func(ctx context.Context) error
	CommitFn   func(ctx context.Context) error
}

func (m mockTx) Rollback(ctx context.Context) error {
	return m.RollbackFn(ctx)
}

func (m mockTx) Commit(ctx context.Context) error {
	return m.CommitFn(ctx)
}
