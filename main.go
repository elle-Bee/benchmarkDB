package main

import (
	ui "benchmarkDB/ui"
	"fmt"
	"os"

	tea "github.com/charmbracelet/bubbletea"
)

func main() {
	p := tea.NewProgram(ui.InitialModel())
	if _, err := p.Run(); err != nil {
		fmt.Printf("Error occured: %v", err)
		os.Exit(1)
	}
}
