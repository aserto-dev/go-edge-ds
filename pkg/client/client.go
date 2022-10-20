package client

import (
	asertogoClient "github.com/aserto-dev/aserto-go/client"
	dse "github.com/aserto-dev/go-directory/aserto/directory/exporter/v2"
	dsi "github.com/aserto-dev/go-directory/aserto/directory/importer/v2"
	dsw "github.com/aserto-dev/go-directory/aserto/directory/writer/v2"
)

type Client struct {
	conn     *asertogoClient.Connection
	Writer   dsw.WriterClient
	Exporter dse.ExporterClient
	Importer dsi.ImporterClient
}

func New(conn *asertogoClient.Connection) (*Client, error) {
	c := Client{
		conn:     conn,
		Writer:   dsw.NewWriterClient(conn.Conn),
		Exporter: dse.NewExporterClient(conn.Conn),
		Importer: dsi.NewImporterClient(conn.Conn),
	}
	return &c, nil
}
