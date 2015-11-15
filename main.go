package main

import (
	"flag"
	"github.com/davecheney/profile"
	"github.com/golang/glog"
)

func main() {
	cfg := profile.Config{
		CPUProfile:     true,
		MemProfile:     true,
		ProfilePath:    ".",  // store profiles in current directory
		NoShutdownHook: true, // do not hook SIGINT
	}
	defer profile.Start(&cfg).Stop()

	outFile := flag.String("output", "result.out", "Output file")
	flag.Parse()

	sortFiles(*outFile, flag.Args()...)
	glog.V(1).Info("ALL DONE.")
}
