package consumer

type Consumer interface {
	Add(data Reportable) error
	Flush() error
	Close() error
}
