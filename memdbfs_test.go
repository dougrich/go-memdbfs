package memdbfs

import (
	"testing"
)

func TestPlaceholder(t *testing.T) {
	if actual := Placeholder(); actual != 0 {
		t.Fatalf("Expected Placeholder() to equal 0, was actually %d", actual)
	}
}
