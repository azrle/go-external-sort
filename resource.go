package main

import (
	"bufio"
	"github.com/golang/glog"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
)

// OK, there is neither getter nor setter
// just hard-coded dirty const.
// fix it later maybe
const (
	ReaderNum = 4
	WorkerNum = 5
	WriterNum = 6
	ChunkSize = 819200
	MaxIntLen = 11
)

type chunk struct {
	data      []int
	str_buf   [MaxIntLen]byte
	chunkPool chan *chunk
}

type genWorker struct {
	jobChan    chan job
	quit       chan struct{}
	workerPool chan *genWorker
}

type resourcePool struct {
	readerPool chan *chunk
	workerPool chan *genWorker
	writerPool chan *chunk
	workerQuit chan struct{}
}

func newResourcePool() *resourcePool {
	return &resourcePool{
		readerPool: make(chan *chunk, ReaderNum),
		workerPool: make(chan *genWorker, WorkerNum),
		writerPool: make(chan *chunk, WriterNum),
		workerQuit: make(chan struct{}),
	}
}

// Initialize limited resources according to configs;
// and start workers.
func (res *resourcePool) init() *resourcePool {
	for i := 0; i < ReaderNum; i++ {
		res.readerPool <- &chunk{data: make([]int, 0, ChunkSize), chunkPool: res.readerPool}
	}
	for i := 0; i < WriterNum; i++ {
		res.writerPool <- &chunk{data: make([]int, 0, ChunkSize), chunkPool: res.writerPool}
	}
	for i := 0; i < WorkerNum; i++ {
		w := &genWorker{
			jobChan:    make(chan job),
			workerPool: res.workerPool,
			quit:       res.workerQuit,
		}
		go w.start()
	}
	return res
}

func (res *resourcePool) destroy() {
	close(res.readerPool)
	close(res.writerPool)
	close(res.workerQuit)
}

// Work will start waiting for jobs;
// and when it get one, job will be executed.
func (w *genWorker) start() {
	for {
		// register itself to worker pool
		w.workerPool <- w
		select {
		case job := <-w.jobChan:
			job.start()
		case <-w.quit:
			return
		}
	}
}

func (c *chunk) isEmpty() bool {
	return len(c.data) == 0
}

func (c *chunk) sort() {
	sort.Ints(c.data)
}

func simpleItoa(b []byte, n int) []byte {
	b[MaxIntLen-1] = byte('\n')
	l := MaxIntLen - 2
	for n > 0 {
		b[l] = byte((n % 10) + 48)
		n /= 10
		l -= 1
	}
	if l == MaxIntLen-2 {
		b[l] = 48
		l -= 1
	}
	return b[l+1:]
}

func (c *chunk) writeFromMerge(c1, c2 *chunk, w *bufio.Writer) error {
	var i, j int

	var out []byte
	if len(c1.data) == 0 && len(c2.data) == 0 {
		return &errMsg{"Nothing to write"}
	}
	if len(c1.data) == 0 {
		for j < len(c2.data) {
			out = simpleItoa(c.str_buf[:], c2.data[j])
			bytes, err := w.Write(out)
			if err != nil || bytes != len(out) {
				return err
			}
			j++
		}
	}
	if len(c2.data) == 0 {
		for i < len(c1.data) {
			out = simpleItoa(c.str_buf[:], c1.data[i])
			bytes, err := w.Write(out)
			if err != nil || bytes != len(out) {
				return err
			}
			i++
		}
	}
	for i < len(c1.data) && j < len(c2.data) {
		if j >= len(c2.data) || (i < len(c1.data) && c1.data[i] < c2.data[j]) {
			out = simpleItoa(c.str_buf[:], c1.data[i])
			i++
		} else {
			out = simpleItoa(c.str_buf[:], c2.data[j])
			j++
		}
		bytes, err := w.Write(out)
		if err != nil || bytes != len(out) {
			return err
		}
	}
	c1.data = c1.data[i:len(c1.data)]
	c2.data = c2.data[j:len(c2.data)]

	return nil
}

func (c *chunk) clone(src *chunk) {
	c.data = append(c.data, src.data...)
}

func (c *chunk) reset() {
	c.data = c.data[0:0]
}

func (c *chunk) read(s *bufio.Scanner, quit <-chan struct{}) error {
	for len(c.data) < cap(c.data) {
		if !s.Scan() {
			break
		}
		if s.Err() != nil {
			return s.Err()
		}
		select {
		case <-quit:
			return &errMsg{"cancel"}
		default:
			r, err := strconv.Atoi(s.Text())
			if err != nil {
				return err
			}
			c.data = append(c.data, r)
		}
	}
	return nil
}

func (c *chunk) asyncWrite(
	prefix string,
	quit <-chan struct{},
	outc chan *os.File,
	errc chan error,
) {
	defer func() { c.chunkPool <- c }()
	defer c.reset()

	out, err := ioutil.TempFile("", prefix)
	if err != nil {
		errc <- err
		return
	}
	glog.V(2).Infof("[Chunk] Start writing to %s", out.Name())
	w := bufio.NewWriter(out)
	defer w.Flush()

	for _, n := range c.data {
		select {
		case <-quit:
			errc <- &errMsg{out.Name() + " canceled"}
			return
		default:
			bytes, err := w.WriteString(strconv.Itoa(n) + "\n")
			if err != nil || bytes != 1+len(strconv.Itoa(n)) {
				errc <- err
				return
			}
		}
	}

	outc <- out
	glog.V(2).Infof("[Chunk] Finished. Send %s", out.Name())
}
