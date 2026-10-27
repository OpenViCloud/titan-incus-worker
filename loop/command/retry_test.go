package command

import (
	"errors"
	"fmt"
	"testing"
)

// permanentAnswer stands for an error from another package that knows no retry can fix it.
type permanentAnswer struct {
	permanent bool
}

func (e *permanentAnswer) Error() string {
	return "answer"
}

func (e *permanentAnswer) Permanent() bool {
	return e.permanent
}

func TestIsRetryable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"a plain error, such as a failed Incus operation", errors.New("operation failed"), true},
		{"marked Permanent here", Permanent(errors.New("bad payload")), false},
		{"marked Permanent, then wrapped", fmt.Errorf("vm.create: %w", Permanent(errors.New("bad payload"))), false},
		{"an error that says it is permanent", &permanentAnswer{permanent: true}, false},
		{"an error that says it is not permanent", &permanentAnswer{permanent: false}, true},
		{"an error that says it is permanent, wrapped twice", fmt.Errorf("cluster c: %w (%w)", &permanentAnswer{permanent: true}, errors.New("404")), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := IsRetryable(c.err)
			if got != c.want {
				t.Fatalf("IsRetryable(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

func TestPermanent_OfNil_IsNil(t *testing.T) {
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) must stay nil so a nil error still reads as success")
	}
}
