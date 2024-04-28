package main

import (
	ui "benchmarkDB/ui"
	"fmt"

	"github.com/gizak/termui/v3"
)

func main() {

	if err := termui.Init(); err != nil {
		fmt.Printf("failed to initialize termui: %v", err)
	}
	defer termui.Close()

	ui.SetupUI()
}
