package impl

import (
	"context"
	"fmt"

	"github.com/leisurelyrcxf/spermwhale/errors"

	"github.com/leisurelyrcxf/spermwhale/proto/oraclepb"
	"google.golang.org/grpc"
)

var NilFetchResponse = fmt.Errorf("nil fetch resp")

type Client struct {
	oracleClient oraclepb.OracleClient
	conn         *grpc.ClientConn
}

func NewClient(serverAddr string) (*Client, error) {
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:         conn,
		oracleClient: oraclepb.NewOracleClient(conn),
	}, nil
}

func (c *Client) FetchTimestamp(ctx context.Context) (uint64, error) {
	resp, err := c.oracleClient.Fetch(ctx, &oraclepb.FetchRequest{})
	if err != nil {
		return 0, err
	}
	if resp == nil {
		return 0, NilFetchResponse
	}
	if resp.Err != nil {
		return 0, errors.NewErrorFromPB(resp.Err)
	}
	return resp.Ts, nil
}

func (c *Client) TargetAddr() string {
	return c.conn.Target()
}

func (c *Client) Close() error {
	return c.conn.Close()
}
