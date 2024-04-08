package datasync

type SyncMode int

const (
	Full SyncMode = iota
	Watermark
)

type config struct {
	mode SyncMode
}

type Option func(*config)

func WithMode(mode SyncMode) Option {
	return func(c *config) {
		c.mode = mode
	}
}
