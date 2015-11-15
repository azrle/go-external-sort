package main

import (
	"github.com/golang/glog"
	"os"
)

const (
	OutChannelSize = 10
	SplitPrefix    = "split."
	MergePrefix    = "merge."
)

type errMsg struct {
	msg string
}

func (e *errMsg) Error() string {
	return e.msg
}

func watchError(jm *jobManager) {
	for err := range jm.err {
		glog.Fatalf("[%s] %s", jm.name, err)
	}
}

// main entry
func sortFiles(outFile string, inFiles ...string) {
	splitter := newJobManager("Split", newResourcePool().init())
	go watchError(splitter)
	merger := newJobManager("Merge", newResourcePool().init())
	go watchError(merger)

	splitter.wg.Add(len(inFiles))
	// split phase
	go func() {
		glog.V(1).Infof("[%s] Started.", splitter.name)
		for _, file := range inFiles {
			w := splitter.getGenWorker()
			w.jobChan <- &splitJob{
				filename: file,
				manager:  splitter,
			}
		}
		splitter.wg.Wait()
		glog.V(1).Infof("[%s] Done.", splitter.name)
		close(splitter.out)
		close(splitter.err)
		close(splitter.quit)
		splitter.res.destroy()
	}()

	// merge phase
	// it may be ok to keep it in main thread
	glog.V(1).Infof("[%s] Started.", merger.name)
	splitter_alive := true
	var file [2]*os.File
	current := 0
	total := 0
MERGER:
	for {
		if splitter_alive {
			select {
			case file[current], splitter_alive = <-splitter.out:
				if !splitter_alive {
					continue
				}
				glog.V(2).Infof(
					"[%s] Recevied from split phase: %s",
					merger.name,
					file[current].Name(),
				)
				current++
				total++
				merger.wg.Add(1)
			case file[current] = <-merger.out:
				glog.V(2).Infof(
					"[%s] Recevied from merge phase: %s",
					merger.name,
					file[current].Name(),
				)
				current++
			}
		} else {
			glog.V(2).Infof(
				"[%s] Split phase is done. Files remained to be merged: %d",
				merger.name,
				total,
			)
			switch total {
			case 0:
				glog.Warningf("[%s] Nothing merged.", merger.name)
				break MERGER
			case 1:
				// we have new putput from splitter
				// which means total will not increase anymore
				// and, we have already performed (total-1)'s merge
				// which means all split files were merged to one

				// Therefore, we are done here.
				if current == 1 {
					current = 0
				} else {
					file[current] = <-merger.out
				}
				file[current].Close()
				glog.V(2).Infof(
					"[%s] Renaming temp result %s to %s",
					merger.name,
					file[current].Name(), outFile,
				)
				err := os.Rename(file[current].Name(), outFile)
				if err != nil {
					merger.err <- err
				}
				merger.wg.Done()
				break MERGER
			default:
				file[current] = <-merger.out
				current++
			}
		}
		if current == 2 {
			current = 0
			total--
			w := merger.getGenWorker()
			w.jobChan <- &mergeJob{
				file1:   file[0],
				file2:   file[1],
				manager: merger,
			}
		}
	}

	merger.wg.Wait()
	close(merger.out)
	close(merger.err)
	close(merger.quit)
	merger.res.destroy()
	glog.V(1).Infof("[%s] Done.", merger.name)
}
