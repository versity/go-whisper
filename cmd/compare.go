package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	whisper "github.com/go-graphite/go-whisper"
)

func init() { log.SetFlags(log.Lshortfile) }

func main() {
	now := flag.Int("now", 0, "specify the current time")
	ignoreBuffer := flag.Bool("ignore-buffer", false, "ignore points in buffer that haven't been propagated")
	quarantinesRaw := flag.String("quarantines", "", "ignore data started from this point. e.g. 2019-02-21,2019-02-22")
	verbose := flag.Bool("verbose", false, "be overly and nicely talkive")
	strict := flag.Bool("strict", false, "exit 1 whenever there are discrepancies between between the files")
	muteThreshold := flag.Int("mute-if-less", 2, "do not alarm if diff of points is less than specified.")
	flag.BoolVar(verbose, "v", false, "be overly and nicely talkive")
	flag.Parse()
	if len(flag.Args()) != 2 {
		fmt.Println("usage: cverify metric.wsp metric.cwsp")
		fmt.Println("purpose: check if two whisper files are containing the same data, made for verify migration result.")
		flag.PrintDefaults()
		os.Exit(1)
	}

	file1 := flag.Args()[0]
	file2 := flag.Args()[1]
	msg, err := whisper.Compare(
		file1, file2,
		*now,
		*ignoreBuffer,
		*quarantinesRaw,
		*verbose,
		*strict,
		*muteThreshold,
	)
	if len(msg) > 0 {
		fmt.Print(msg)
	}
	if err != nil {
		fmt.Print(err)
		os.Exit(1)
	}
}
