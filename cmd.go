package rkv

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// CmdType identifies different command types.
type CmdType string

// Command types.
const (
	CmdRegister CmdType = "REG"
	CmdInsert   CmdType = "INS"
)

// Cmd holds the parsed command info. CmdType should be checked to see which
// fields are accessible.
type Cmd struct {
	Cmd     string // Uniquely identifies CmdRegister command.
	CmdType CmdType

	// Uniquely identifies CmdInsert command.
	ID    string
	Seq   uint64
	Key   string
	Value string
}

// ErrMalformedCmd indicates the command being parsed is malformed.
var ErrMalformedCmd = errors.New("malformed cmd")

// NewCmdRegister creates a new CmdRegister command.
func NewCmdRegister(uid int64) (Cmd, error) {
	return ParseCmd(fmt.Sprintf("%s:%d", CmdRegister, uid))
}

// NewCmdInsert creates a new CmdInsert command.
func NewCmdInsert(id, seq, key, value string) (Cmd, error) {
	return ParseCmd(fmt.Sprintf("%s:%s,%s,%s,%s",
		CmdInsert,
		id, seq, key, value))
}

// ParseCmd returns a Cmd from the string given.
func ParseCmd(cmd string) (Cmd, error) {
	s := strings.Split(cmd, ":")

	if len(s) != 2 {
		return Cmd{}, ErrMalformedCmd
	}

	c := Cmd{
		Cmd:     cmd,
		CmdType: CmdType(s[0]),
	}

	switch c.CmdType {
	case CmdRegister:
		return c, nil
	case CmdInsert:
		s := strings.Split(s[1], ",")
		if len(s) != 4 {
			return Cmd{}, ErrMalformedCmd
		}

		c.ID = s[0]
		seq, err := strconv.Atoi(s[1])
		if err != nil {
			return Cmd{}, ErrMalformedCmd
		}
		c.Seq = uint64(seq)
		c.Key = s[2]
		c.Value = s[3]

		return c, nil
	default:
		return Cmd{}, ErrMalformedCmd
	}
}

func (c Cmd) String() string {
	return c.Cmd
}
