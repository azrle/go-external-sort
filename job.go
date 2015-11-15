package main

import (
	"bufio"
	"github.com/golang/glog"
	"io/ioutil"
	"os"
	"sync"
)

type job interface {
	start()
}

type splitJob struct {
	filename string
	manager  *jobManager
}

type mergeJob struct {
	file1   *os.File
	file2   *os.File
	manager *jobManager
}

type jobManager struct {
	name string
	res  *resourcePool
	err  chan error
	out  chan *os.File
	quit chan struct{}
	wg   sync.WaitGroup
}

func newJobManager(name string, r *resourcePool) *jobManager {
	return &jobManager{
		name: name,
		res:  r,
		err:  make(chan error),
		out:  make(chan *os.File, OutChannelSize),
		quit: make(chan struct{}),
	}
}

func (jm *jobManager) getGenWorker() *genWorker {
	w := <-jm.res.workerPool
	return w
}

// Split a file to sorted chunks
func (j *splitJob) start() {
	glog.V(2).Infof("[%s] Job started: split %s to sorted chunks", j.manager.name, j.filename)
	defer j.manager.wg.Done()

	in, err := os.Open(j.filename)
	if err != nil {
		j.manager.err <- err
		return
	}
	defer in.Close()

	r_chunk := <-j.manager.res.readerPool
	defer func() {
		r_chunk.reset()
		r_chunk.chunkPool <- r_chunk
	}()

	s := bufio.NewScanner(in)
	for {
		err := r_chunk.read(s, j.manager.quit)
		if err != nil {
			j.manager.err <- err
			return
		}
		if r_chunk.isEmpty() {
			break
		}
		r_chunk.sort()

		w_chunk := <-j.manager.res.writerPool
		w_chunk.clone(r_chunk)
		w_chunk.asyncWrite(
			SplitPrefix,
			j.manager.quit,
			j.manager.out,
			j.manager.err,
		)

		r_chunk.reset()
	}
}

// Merge sorted chunks
func (j *mergeJob) start() {
	glog.V(2).Infof("[%s] Job started: merge %s %s",
		j.manager.name, j.file1.Name(), j.file2.Name(),
	)
	defer j.manager.wg.Done()
	r_chunk := <-j.manager.res.readerPool
	w_chunk := <-j.manager.res.writerPool

	defer func() {
		r_chunk.reset()
		r_chunk.chunkPool <- r_chunk
		w_chunk.reset()
		w_chunk.chunkPool <- w_chunk

		j.file1.Close()
		err := os.Remove(j.file1.Name())
		if err != nil {
			j.manager.err <- err
		}
		j.file2.Close()
		err = os.Remove(j.file2.Name())
		if err != nil {
			j.manager.err <- err
		}
	}()

	half := cap(r_chunk.data) / 2
	c1 := &chunk{}
	c2 := &chunk{}

	_, err := j.file1.Seek(0, 0)
	if err != nil {
		j.manager.err <- err
		return
	}
	s1 := bufio.NewScanner(j.file1)
	_, err = j.file2.Seek(0, 0)
	if err != nil {
		j.manager.err <- err
		return
	}
	s2 := bufio.NewScanner(j.file2)

	outFile, err := ioutil.TempFile("", MergePrefix)
	if err != nil {
		j.manager.err <- err
		return
	}
	glog.V(2).Infof("[%s] Merge output: opened %s", j.manager.name, outFile.Name())
	writer := bufio.NewWriter(outFile)

	for {
		if c1.isEmpty() {
			if cap(c1.data) == 0 {
				c1.data = r_chunk.data[0:0:half]
			}
			err = c1.read(s1, j.manager.quit)
			if err != nil {
				j.manager.err <- err
				return
			}
		}
		if c2.isEmpty() {
			if cap(c2.data) == 0 {
				c2.data = r_chunk.data[half:half:cap(r_chunk.data)]
			}
			err = c2.read(s2, j.manager.quit)
			if err != nil {
				j.manager.err <- err
				return
			}
		}
		if c1.isEmpty() && c2.isEmpty() {
			break
		}
		// we cannot peform an asyncWrite here
		// since we have to keep data in order
		glog.V(2).Infof("[%s] Merge output: write a chunk to %s (len %d + %d)",
			j.manager.name, outFile.Name(), len(c1.data), len(c2.data),
		)
		err = w_chunk.writeFromMerge(c1, c2, writer)
		if err != nil {
			j.manager.err <- err
			break
		}
	}

	writer.Flush()
	j.manager.out <- outFile
	glog.V(2).Infof("[%s] Job done: merge %s %s",
		j.manager.name, j.file1.Name(), j.file2.Name(),
	)
}
