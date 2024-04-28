package ui

import (
	"fmt"

	"github.com/gizak/termui/v3"
	"github.com/gizak/termui/v3/widgets"
)

func SetupUI() {

	p := widgets.NewParagraph()
	p.Text = "Please select an operation!"

	operations := []string{"Create", "Read", "Update", "Delete"}

	l := widgets.NewList()
	l.Title = "Operations"
	l.Rows = operations
	l.TextStyle = termui.NewStyle(termui.ColorWhite)
	l.WrapText = false
	l.SelectedRow = 0
	l.SelectedRowStyle = termui.NewStyle(termui.ColorWhite, termui.ColorBlack)
	l.SetRect(0, 2, 30, len(operations)+2)

	grid := termui.NewGrid()
	grid.SetRect(0, 0, 50, len(operations)+4)
	grid.Set(
		termui.NewRow(1.0,
			termui.NewCol(1.0, p),
			termui.NewCol(1.0, l),
		),
	)

	termui.Render(grid)
	for _, operation := range operations {
		fmt.Println(operation)
	}

	termuiEvents := termui.PollEvents()
	for {
		select {
		case e := <-termuiEvents:
			if e.Type == termui.KeyboardEvent {
				switch e.ID {
				case "<Up>":
					l.ScrollUp()
					termui.Render(grid)
				case "<Down>":
					l.ScrollDown()
					termui.Render(grid)
				case "<Enter>":
					selectedIndex := l.SelectedRow
					p.Text = "Selected: " + operations[selectedIndex]
					termui.Render(grid)
				case "q", "Q":
					return
				}
			}
		}
	}
}
