package main

import (
	"fmt"
	"os"

	cli "github.com/urfave/cli/v2"
	esl "github.com/yeqown/enchanted-sleeve"
)

// esl-ctl is a command line tool to control enchanted-sleeve.
// Usage:
// $ esl-ctl sub-command [global flags] [sub-command flags] [args...]
// It has sub-commands:
// - get: esl-ctl get [global flags] key
// - set: esl-ctl set [global flags] key value
// - del: esl-ctl del [global flags] key
// - keys: esl-ctl keys [global flags]
//
// Global flags:
// - path: path to db, default is ./testdata

func main() {
	app := newCliApp()
	if err := app.Run(os.Args); err != nil {
		fmt.Printf("esl-ctl failed: %v\n", err)
	}
}

func newCliApp() *cli.App {
	app := cli.NewApp()
	app.Name = "esl-ctl"
	app.Usage = "enchanted-sleeve control tool"
	app.Version = "0.0.1"
	app.Commands = []*cli.Command{
		newGetCommand(),
		newSetCommand(),
		newDelCommand(),
		newKeysCommand(),
	}
	app.Before = func(c *cli.Context) error {
		dbpath := c.String("path")
		db, err := esl.Open(dbpath)
		if err != nil {
			return err
		}

		_ = db
		// TODO: set db into context
		return nil
	}
	// global flags
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:     "path",
			Aliases:  []string{"p"},
			Usage:    "path to db",
			Value:    "./testdata",
			Required: true,
		},
	}

	return app
}

func newGetCommand() *cli.Command {
	var db *esl.DB
	_ = db
	return &cli.Command{
		Name:  "get",
		Usage: "get value by the input key",
		Action: func(c *cli.Context) error {
			fmt.Println("get command running")
			return nil
		},
	}
}

func newSetCommand() *cli.Command {
	return &cli.Command{
		Name:  "set",
		Usage: "set key-value pair",
		Action: func(c *cli.Context) error {
			fmt.Println("set command running")
			return nil
		},
	}
}

func newDelCommand() *cli.Command {
	return &cli.Command{
		Name:  "del",
		Usage: "delete key-value pair",
		Action: func(c *cli.Context) error {
			fmt.Println("del command running")
			return nil
		},
	}
}

func newKeysCommand() *cli.Command {
	return &cli.Command{
		Name:  "keys",
		Usage: "list all keys",
		Action: func(c *cli.Context) error {
			fmt.Println("keys command running")
			return nil
		},
	}
}
