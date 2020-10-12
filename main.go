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

const DataCount = 30             // How much moto in json file
const RoutineCount = 10          // How much worker routines to start
const BufferSize = DataCount / 2 // Size of DataMonitor internal buffer
const FilterCriteria = 26        // Select cars whose aging value is less

type (
	Car struct {
		Make    string  `json:"make"`
		Year    int     `json:"year"`
		Mileage float64 `json:"mileage"`
	}

	CarWithAge struct {
		Car Car
		Age int
	}

	DataMonitor struct {
		Cars                  [BufferSize]Car
		In, Out               int
		Work, Space           *sync.Cond
		WorkCount, SpaceCount int
		InputLock, OutputLock sync.Mutex
	}

	SortedResultMonitor struct {
		Cars  [DataCount]CarWithAge
		Count int
		Lock  sync.Mutex
	}
)



func main() {
	dataMonitor := NewDataMonitor()
	resultMonitor := NewSortedResultMonitor()
	var wg sync.WaitGroup
	wg.Add(RoutineCount)

	cars := readData("IFK-8_OdinasT_L1_dat_1.json")
	startWorkers(dataMonitor, resultMonitor, RoutineCount, &wg)
	fillDataMonitor(&cars, dataMonitor)
	wg.Wait()
	writeData("IFK-8_OdinasT_L1_rez.txt", &cars, resultMonitor)
}

func NewSortedResultMonitor() *SortedResultMonitor { return &SortedResultMonitor{} }
func NewDataMonitor() *DataMonitor {
	monitor := DataMonitor{SpaceCount: BufferSize}
	monitor.Work = sync.NewCond(&monitor.OutputLock)
	monitor.Space = sync.NewCond(&monitor.InputLock)
	return &monitor
}

func (m *DataMonitor) addItem(item Car) {
	m.InputLock.Lock()
	for m.SpaceCount < 1{
		m.Space.Wait()
	}
	m.Cars[m.In] = item
	m.In = (m.In + 1) % BufferSize
	m.SpaceCount--
	m.InputLock.Unlock()

	m.OutputLock.Lock() // could be 1 line atomic
	m.WorkCount++
	m.OutputLock.Unlock()

	m.Work.Signal()
}

func (m *DataMonitor) removeItem() Car {
	m.OutputLock.Lock()
	for m.WorkCount < 1 {
		m.Work.Wait()
	}

	car := m.Cars[m.Out]
	if car.Make == "<EndOfInput>" { // Poison pill pattern
		m.OutputLock.Unlock()
		m.Work.Signal()
		return car
	}

	m.Out = (m.Out + 1) % BufferSize
	m.WorkCount--
	m.OutputLock.Unlock()

	m.InputLock.Lock()
	m.SpaceCount++
	m.InputLock.Unlock()
	m.Space.Signal()
	return car
}

// Computes custom car aging value
func (c *Car) Age() int {
	return time.Now().Year() - c.Year + int(c.Mileage/20_000)
}

func fillDataMonitor(cars *[DataCount]Car, dataMonitor *DataMonitor) {
	for _, car := range cars {
		dataMonitor.addItem(car) // Will block if no space
	}
	// Inform consumers that there will be no more data coming
	dataMonitor.addItem(Car{Make: "<EndOfInput>"})
}

func startWorkers(dataMonitor *DataMonitor, resultMonitor *SortedResultMonitor, workerCount int, wg *sync.WaitGroup) {
	for i := 0; i < workerCount; i++ {
		go worker(dataMonitor, resultMonitor, wg)
	}
}

func worker(in *DataMonitor, out *SortedResultMonitor, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		car := in.removeItem() // Will block if no data
		if car.Make == "<EndOfInput>" {
			break
		}

		carAge := car.Age()
		if carAge < FilterCriteria {
			car := CarWithAge{car, carAge}
			out.addItemSorted(car)
		}
	}
}

// Sort by car age ascending, and then by year descending
func (m *SortedResultMonitor) addItemSorted(car CarWithAge) {
	m.Lock.Lock()
	i := m.Count - 1
	for i >= 0 && (m.Cars[i].Age > car.Age || (m.Cars[i].Age == car.Age && m.Cars[i].Car.Year < car.Car.Year)) {
		m.Cars[i+1] = m.Cars[i]
		i--
	}
	m.Cars[i+1] = car
	m.Count++
	m.Lock.Unlock()
}

func readData(path string) [DataCount]Car {
	data, _ := ioutil.ReadFile(path)
	var cars [DataCount]Car
	_ = json.Unmarshal(data, &cars)
	return cars
}

func writeData(path string, inputData *[DataCount]Car, results *SortedResultMonitor) {
	file, _ := os.Create(path)
	defer file.Close()

	_, _ = fmt.Fprint(file, strings.Repeat("━", 42)+"\n")
	_, _ = fmt.Fprintf(file, "┃%25s%16s\n", "INPUT DATA", "┃")
	_, _ = fmt.Fprint(file, strings.Repeat("━", 42)+"\n")
	_, _ = fmt.Fprintf(file, "┃%-13s┃%10s┃%15s┃\n", "Make", "Year", "Mileage")
	_, _ = fmt.Fprint(file, strings.Repeat("━", 42)+"\n")
	for _, car := range inputData {
		_, _ = fmt.Fprintf(file, "┃%-13s┃%10d┃%15.2f┃\n", car.Make, car.Year, car.Mileage)
	}
	_, _ = fmt.Fprint(file, strings.Repeat("━", 42)+"\n\n")

	_, _ = fmt.Fprint(file, strings.Repeat("━", 48)+"\n")
	_, _ = fmt.Fprintf(file, "┃%29s%18s\n", "OUTPUT DATA", "┃")
	_, _ = fmt.Fprint(file, strings.Repeat("━", 48)+"\n")
	_, _ = fmt.Fprintf(file, "┃%-13s┃%10s┃%15s┃%5s┃\n", "Make", "Year", "Mileage", "Age")
	_, _ = fmt.Fprint(file, strings.Repeat("━", 48)+"\n")
	for i := 0; i < results.Count; i++ {
		data := results.Cars[i]
		_, _ = fmt.Fprintf(file, "┃%-13s┃%10d┃%15.2f┃%5d┃\n", data.Car.Make, data.Car.Year, data.Car.Mileage, data.Age)
	}
	_, _ = fmt.Fprint(file, strings.Repeat("━", 48)+"\n")
}