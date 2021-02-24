package txn

import (
	"github.com/leisurelyrcxf/spermwhale/tablet"
	"github.com/leisurelyrcxf/spermwhale/time2"
)

type TransactionManager struct {
	client *tablet.Client
	tm     *time2.TimeServer
}

func NewTransactionManager(tabletAddr string) (*TransactionManager, error) {
	cli, err := tablet.NewClient(tabletAddr)
	if err != nil {
		return nil, err
	}
	return &TransactionManager{
		client: cli,
		tm:     time2.NewTimeServer(),
	}, nil
}

func (m *TransactionManager) Close() error {
	return m.client.Close()
}

func (m *TransactionManager) BeginTxn() (*Txn, error) {

}

func (m *TransactionManager) GetTxn(version uint64) (*Txn, error) {
	return nil, nil

}
