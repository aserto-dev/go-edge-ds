package directory

import dse "github.com/aserto-dev/go-directory/aserto/directory/exporter/v2"

func (s *Directory) Export(*dse.ExportRequest, dse.Exporter_ExportServer) error {
	return nil
}
