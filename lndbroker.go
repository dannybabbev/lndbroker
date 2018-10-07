package lndbroker

import (
	"context"
	"encoding/hex"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"io/ioutil"
)

type LNDBroker struct {
	LNclient lnrpc.LightningClient
	Macaroon string
}

// NewBroker returns an LNDBroker instance
func NewBroker(nodeAddr string, certFile string, macaroonFile string) (*LNDBroker, error) {
	// Connect to the lightning node
	creds, _ := credentials.NewClientTLSFromFile(certFile, "")

	// Set up a connection to the server.
	lnConn, err := grpc.Dial(nodeAddr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, err
	}

	macaroon, err := ioutil.ReadFile(macaroonFile)
	if err != nil {
		return nil, err
	}

	return &LNDBroker{
		LNclient: lnrpc.NewLightningClient(lnConn),
		Macaroon: hex.EncodeToString(macaroon),
	}, nil
}

// GetStdContext returns the grpc context with the correct macaroon set
func (b *LNDBroker) GetStdContext() context.Context {
	meta := metadata.Pairs("macaroon", b.Macaroon)
	return metadata.NewOutgoingContext(context.Background(), meta)
}

func (b *LNDBroker) GetInfo() (*lnrpc.GetInfoResponse, error) {
	return b.LNclient.GetInfo(b.GetStdContext(), &lnrpc.GetInfoRequest{})
}

// Subscribe transactions binds a channel to incoming on-chain transactions
func (b *LNDBroker) SubscribeTransactions(out chan<- *lnrpc.Transaction) error {
	client, err := b.LNclient.SubscribeTransactions(b.GetStdContext(), &lnrpc.GetTransactionsRequest{})
	if err != nil {
		return err
	}

	for {
		tr, err := client.Recv()
		if err != nil {
			logrus.WithField("err", err).Warn("Error receiving transaction")
			return err
		}

		out <- tr
	}
}

// Subscribe invoices binds a channel to incoming invoices
func (b *LNDBroker) SubscribeInvoices(out chan<- *lnrpc.Invoice) error {
	client, err := b.LNclient.SubscribeInvoices(b.GetStdContext(), &lnrpc.InvoiceSubscription{})
	if err != nil {
		return err
	}

	for {
		tr, err := client.Recv()
		if err != nil {
			logrus.WithField("err", err).Warn("Error receiving transaction")
			return err
		}

		out <- tr
	}
}
