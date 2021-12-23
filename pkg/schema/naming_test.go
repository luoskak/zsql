package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToDBName(t *testing.T) {
	namer := &NamingStrategy{}
	assert.Equal(t, namer.toDBName("Day_2"), "day_2")
}
