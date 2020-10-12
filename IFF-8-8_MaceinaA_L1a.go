package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"
)

const DataNumber = 30             // How much moto in json file
const WorkerCount = 10            // How much worker routines to start
const BufferSize = DataNumber / 2 // Size of DataMonitor internal buffer
const Criteria = 26               // Select moto whose purchase value is less

type (
	Moto struct {
		Manufacturer string  `json:"manufacturer"`
		Date         int     `json:"date"`
		Distance     float64 `json:"distance"`
	}

	MotoRank struct {
		Moto Moto
		Rank int
	}

	DataMonitor struct {
		Motos                 [BufferSize]Moto
		In, Out               int
		Work, Space           *sync.Cond
		WorkCount, SpaceCount int
		InputLock, OutputLock sync.Mutex
	}

	SortedResultMonitor struct {
		Motos [DataNumber]MotoRank
		Count int
		Lock  sync.Mutex
	}
)

func main() {
	dataMonitor := NewDataMonitor()
	resultMonitor := NewSortedResultMonitor()
	var wg sync.WaitGroup
	wg.Add(WorkerCount)

	motos := readData("IFF-8-8_MaceinaA_L1_dat_1.json")
	startWorkers(dataMonitor, resultMonitor, WorkerCount, &wg)
	fillDataMonitor(&motos, dataMonitor)
	wg.Wait()
	writeData("IFF-8-8_MaceinaA_L1_rez.txt", &motos, resultMonitor)
}

func NewSortedResultMonitor() *SortedResultMonitor { return &SortedResultMonitor{} }
func NewDataMonitor() *DataMonitor {
	monitor := DataMonitor{SpaceCount: BufferSize}
	monitor.Work = sync.NewCond(&monitor.OutputLock)
	monitor.Space = sync.NewCond(&monitor.InputLock)
	return &monitor
}

func (m *DataMonitor) addItem(item Moto) {
	m.InputLock.Lock()
	for m.SpaceCount < 1 {
		m.Space.Wait()
	}
	m.Motos[m.In] = item
	m.In = (m.In + 1) % BufferSize
	m.SpaceCount--
	m.InputLock.Unlock()

	m.OutputLock.Lock()
	m.WorkCount++
	m.OutputLock.Unlock()

	m.Work.Signal()
}

func (m *DataMonitor) removeItem() Moto {
	m.OutputLock.Lock()
	for m.WorkCount < 1 {
		m.Work.Wait()
	}

	moto := m.Motos[m.Out]
	if moto.Manufacturer == "<EndOfInput>" {
		m.OutputLock.Unlock()
		m.Work.Signal()
		return moto
	}

	m.Out = (m.Out + 1) % BufferSize
	m.WorkCount--
	m.OutputLock.Unlock()

	m.InputLock.Lock()
	m.SpaceCount++
	m.InputLock.Unlock()
	m.Space.Signal()
	return moto
}

// Computes custom moto purchase rank
func (m *Moto) BestMotoRank() int {
	return time.Now().Year() - m.Date + int(m.Distance/1_000)
}

func fillDataMonitor(motos *[DataNumber]Moto, dataMonitor *DataMonitor) {
	for _, moto := range motos {
		dataMonitor.addItem(moto)
	}
	dataMonitor.addItem(Moto{Manufacturer: "<EndOfInput>"})
}

func startWorkers(dataMonitor *DataMonitor, resultMonitor *SortedResultMonitor, workerCount int, wg *sync.WaitGroup) {
	for i := 0; i < workerCount; i++ {
		go worker(dataMonitor, resultMonitor, wg)
	}
}

func worker(in *DataMonitor, out *SortedResultMonitor, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		moto := in.removeItem()
		if moto.Manufacturer == "<EndOfInput>" {
			break
		}

		motoRank := moto.BestMotoRank()
		if motoRank < Criteria {
			moto := MotoRank{moto, motoRank}
			out.addItemSorted(moto)
		}
	}
}

// Sort by moto age ascending, and then by year descending
func (m *SortedResultMonitor) addItemSorted(moto MotoRank) {
	m.Lock.Lock()
	i := m.Count - 1
	for i >= 0 && (m.Motos[i].Rank > moto.Rank || (m.Motos[i].Rank == moto.Rank && m.Motos[i].Moto.Date < moto.Moto.Date)) {
		m.Motos[i+1] = m.Motos[i]
		i--
	}
	m.Motos[i+1] = moto
	m.Count++
	m.Lock.Unlock()
}

func readData(path string) [DataNumber]Moto {
	data, _ := ioutil.ReadFile(path)
	var motos [DataNumber]Moto
	_ = json.Unmarshal(data, &motos)
	return motos
}

func writeData(path string, inputData *[DataNumber]Moto, results *SortedResultMonitor) {
	file, _ := os.Create(path)
	defer file.Close()

	_, _ = fmt.Fprint(file, strings.Repeat("━", 42)+"\n")
	_, _ = fmt.Fprintf(file, "┃%25s%16s\n", "INPUT DATA", "┃")
	_, _ = fmt.Fprint(file, strings.Repeat("━", 42)+"\n")
	_, _ = fmt.Fprintf(file, "┃%-13s┃%10s┃%15s┃\n", "Manufacturer", "Date", "Distance")
	_, _ = fmt.Fprint(file, strings.Repeat("━", 42)+"\n")
	for _, moto := range inputData {
		_, _ = fmt.Fprintf(file, "┃%-13s┃%10d┃%15.2f┃\n", moto.Manufacturer, moto.Date, moto.Distance)
	}
	_, _ = fmt.Fprint(file, strings.Repeat("━", 42)+"\n\n")

	_, _ = fmt.Fprint(file, strings.Repeat("━", 48)+"\n")
	_, _ = fmt.Fprintf(file, "┃%29s%18s\n", "OUTPUT DATA", "┃")
	_, _ = fmt.Fprint(file, strings.Repeat("━", 48)+"\n")
	_, _ = fmt.Fprintf(file, "┃%-13s┃%10s┃%15s┃%5s┃\n", "Manufacturer", "Date", "Distance", "Rank")
	_, _ = fmt.Fprint(file, strings.Repeat("━", 48)+"\n")
	for i := 0; i < results.Count; i++ {
		data := results.Motos[i]
		_, _ = fmt.Fprintf(file, "┃%-13s┃%10d┃%15.2f┃%5d┃\n", data.Moto.Manufacturer, data.Moto.Date, data.Moto.Distance, data.Rank)
	}
	_, _ = fmt.Fprint(file, strings.Repeat("━", 48)+"\n")
}
