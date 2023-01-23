package main

import "testing"

func TestInc(t *testing.T) {
	testCases := []struct {
		input string
		want  string
	}{
		{input: "", want: "\x00"},
		{input: "\x00", want: "\x01"},
		{input: "\xfe", want: "\xff"},
		{input: "\xff", want: "\x00\x00"},
		{input: "\x00\xff", want: "\x01\x00"},
		{input: "\x01\x00", want: "\x01\x01"},
		{input: "\xfe\xff", want: "\xff\x00"},
		{input: "\xff\xff", want: "\x00\x00\x00"},
	}
	for _, tc := range testCases {
		got := string(inc([]byte(tc.input)))
		if got != tc.want {
			t.Errorf("inc(%q) = %q, want %q", []byte(tc.input), []byte(got), []byte(tc.want))
		}
	}
}
