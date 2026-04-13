package fetcher

import (
	"embed"
)

//go:embed testdata/*
var testLists embed.FS
