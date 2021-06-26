package elock

type Err string

func (e Err) Error() string {
	return string(e)
}

const (
	ErrAcquire Err = "fail to acquire"
	ErrRelease Err = "fail to release"
)