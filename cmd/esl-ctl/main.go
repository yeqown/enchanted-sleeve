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
// - get:  esl-ctl get  [global flags] key
// - set:  esl-ctl set  [global flags] key value
// - del:  esl-ctl del  [global flags] key
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
	app.Before = func(c *cli.Context) error {
		dbpath := c.String("path")
		db, err := esl.Open(dbpath)
		if err != nil {
			return err
		}

		// set into context
		c.Context = contextWithDB(c.Context, db)

		return nil
	}
	// global flags
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:     "path",
			Aliases:  []string{"p"},
			Usage:    "path to db",
			Value:    "./esldb",
			Required: true,
		},
	}

	app.Commands = []*cli.Command{
		newGetCommand(),
		newSetCommand(),
		newDelCommand(),
		newKeysCommand(),
	}

	return app
}

func newGetCommand() *cli.Command {
	return &cli.Command{
		Name:            "get",
		Usage:           "read key-value pair from db",
		ArgsUsage:       `[key]`,
		SkipFlagParsing: true,
		Action: func(c *cli.Context) error {
			db := dbFromContext(c.Context)
			key := c.Args().First()
			if key == "" {
				return fmt.Errorf("key is empty")
			}

			value, err := db.Get([]byte(key))
			if err != nil {
				return err
			}

			fmt.Printf("key: %s, value: %s\n", key, value)
			return nil
		},
	}
}

func newSetCommand() *cli.Command {
	return &cli.Command{
		Name:  "set",
		Usage: "set key-value pair",
		ArgsUsage: `key: key to set value into db
value: value to set into db`,
		Action: func(c *cli.Context) error {
			db := dbFromContext(c.Context)
			key := c.Args().Get(0)
			value := c.Args().Get(1)
			if key == "" || value == "" {
				return fmt.Errorf("key or value is empty")
			}

			if err := db.Put([]byte(key), []byte(value)); err != nil {
				return err
			}

			fmt.Printf("set key: %s, value: %s\n", key, value)
			return nil
		},
	}
}

func newDelCommand() *cli.Command {
	return &cli.Command{
		Name:  "del",
		Usage: "delete key-value pair",
		Action: func(c *cli.Context) error {
			db := dbFromContext(c.Context)
			key := c.Args().First()
			if key == "" {
				return fmt.Errorf("key is empty")
			}

			if err := db.Delete([]byte(key)); err != nil {
				return err
			}

			fmt.Printf("delete key: %s\n", key)
			return nil
		},
	}
}

func newKeysCommand() *cli.Command {
	return &cli.Command{
		Name:  "keys",
		Usage: "list all keys",
		Action: func(c *cli.Context) error {
			db := dbFromContext(c.Context)
			keys := db.ListKeys()

			fmt.Printf("keys: \n")
			for _, key := range keys {
				fmt.Printf("\t%s\n", key)
			}
			return nil
		},
	}
}
