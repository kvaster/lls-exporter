package lls

import (
	"context"
	"errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.bug.st/serial"
	"lls_exporter/log"
	"net/http"
	"sort"
	"time"
)

const promPrefix = "lls_fls_"

var stopError = errors.New("stop")

type sensorLevel struct {
	Raw    int
	Liters float64
}

type sensor struct {
	address int

	temp          prometheus.Gauge
	rawLevel      prometheus.Gauge
	litersLevel   prometheus.Gauge
	errLineDamage prometheus.Gauge
	errWater      prometheus.Gauge

	levels []sensorLevel
}

type SensorConfig struct {
	Name    string
	Address int
	MinRaw  int
	MaxRaw  int
	Levels  []sensorLevel
}

type SensorsConfig struct {
	Device       string
	ReadDelaySec int
	Sensors      []SensorConfig
}

func (s *SensorsConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	s.Device = "/dev/ttySC0"
	s.ReadDelaySec = 1
	type plain SensorsConfig
	return unmarshal((*plain)(s))
}

func (s *SensorConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	s.MinRaw = 1
	s.MaxRaw = 4094
	type plain SensorConfig
	return unmarshal((*plain)(s))
}

type Exporter struct {
	errChan  chan error
	stopChan chan struct{}
}

func crc8one(data byte, crc byte) byte {
	i := data ^ crc
	crc = 0
	if (i & 0x01) != 0 {
		crc ^= 0x5e
	}
	if (i & 0x02) != 0 {
		crc ^= 0xbc
	}
	if (i & 0x04) != 0 {
		crc ^= 0x61
	}
	if (i & 0x08) != 0 {
		crc ^= 0xc2
	}
	if (i & 0x10) != 0 {
		crc ^= 0x9d
	}
	if (i & 0x20) != 0 {
		crc ^= 0x23
	}
	if (i & 0x40) != 0 {
		crc ^= 0x46
	}
	if (i & 0x80) != 0 {
		crc ^= 0x8c
	}
	return crc
}

func crc8(data []byte) byte {
	var crc byte = 0
	for _, d := range data {
		crc = crc8one(d, crc)
	}
	return crc
}

func New() *Exporter {
	return &Exporter{
		errChan:  make(chan error),
		stopChan: make(chan struct{}),
	}
}

func (e *Exporter) Serve(config SensorsConfig) error {
	var sensors []*sensor
	var sensorsMap = make(map[int]*sensor)

	for _, s := range config.Sensors {
		sensor := &sensor{address: s.Address}

		labels := prometheus.Labels{"name": s.Name}

		sensor.temp = prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        promPrefix + "temperature_celsius",
			Help:        "Current temperature of fuel level sensor.",
			ConstLabels: labels,
		})
		sensor.rawLevel = prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        promPrefix + "fuel_level_raw",
			Help:        "Current raw level of fuel level sensor.",
			ConstLabels: labels,
		})
		sensor.litersLevel = prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        promPrefix + "fuel_level_liters",
			Help:        "Current level of fuel sensor in liters",
			ConstLabels: labels,
		})
		sensor.errLineDamage = prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        promPrefix + "error_line_damaged",
			Help:        "Line damage error",
			ConstLabels: labels,
		})
		sensor.errWater = prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        promPrefix + "error_water",
			Help:        "Water in sensor error",
			ConstLabels: labels,
		})

		prometheus.MustRegister(sensor.temp)
		prometheus.MustRegister(sensor.rawLevel)
		prometheus.MustRegister(sensor.litersLevel)
		prometheus.MustRegister(sensor.errLineDamage)
		prometheus.MustRegister(sensor.errWater)

		if len(s.Levels) < 2 {
			return errors.New("at least two levels should be defined")
		}

		sensor.levels = make([]sensorLevel, len(s.Levels))
		for i, l := range s.Levels {
			nl := &sensor.levels[i]
			nl.Liters = l.Liters
			// remap to 1..4094
			nl.Raw = (l.Raw-s.MinRaw)*4093/(s.MaxRaw-s.MinRaw) + 1
		}
		sort.Slice(sensor.levels, func(i, j int) bool {
			return sensor.levels[i].Raw < sensor.levels[j].Raw
		})

		for i := 0; i < len(sensor.levels)-1; i++ {
			if sensor.levels[i].Liters >= sensor.levels[i+1].Liters {
				return errors.New("sensor raw and liters values are not increasing")
			}
		}

		if sensor.levels[0].Raw != 1 {
			return errors.New("lowest level should be for empty tank")
		}
		if sensor.levels[len(sensor.levels)-1].Raw != 4094 {
			return errors.New("highest level should be for full tank")
		}

		sensors = append(sensors, sensor)
		sensorsMap[sensor.address] = sensor
	}

	mode := &serial.Mode{
		//BaudRate: 115200,
		BaudRate: 19200,
	}
	port, err := serial.Open(config.Device, mode)
	if err != nil {
		return errors.New("error opening rs485 device")
	}

	var serveMux http.ServeMux
	serveMux.Handle("/metrics", promhttp.Handler())
	httpServer := http.Server{Addr: ":8080", Handler: &serveMux}
	go func() {
		log.Info("http serve started")
		defer log.Info("http serve stopped")

		err := httpServer.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			e.onError(err)
		}
	}()

	go func() {
		log.Info("read started")
		defer log.Info("read stopped")

		buf := make([]byte, 9) // we don't need too many bytes, 9 is maximum for our packets

		for {
			r := 0
			for {
				n, err := port.Read(buf[r:])
				if err != nil {
					e.onError(err)
					return
				}
				r += n

				if buf[0] != 0x3e {
					break
				}

				if r < 3 {
					continue
				}

				if buf[2] != 0x06 {
					break
				}

				if r < 9 {
					continue
				}

				if buf[8] != crc8(buf[:8]) {
					break
				}

				address := int(buf[1])
				temp := int(buf[3])
				rawLevel := int(buf[4]) | (int(buf[5]) << 8)

				log.
					WithField("address", address).
					WithField("temp", temp).
					WithField("level", rawLevel).
					Info("data received")

				if rawLevel < 0 || rawLevel > 4095 {
					rawLevel = 0
				}

				s := sensorsMap[address]
				if s != nil {
					damageErr := 0
					waterErr := 0
					var level float64 = 0
					if rawLevel == 0 {
						damageErr = 1
					} else if rawLevel == 4095 {
						waterErr = 0
					} else {
						for i, l := range s.levels {
							if rawLevel == l.Raw {
								level = l.Liters
							} else if rawLevel < l.Raw {
								pl := s.levels[i-1]
								level = float64(rawLevel-pl.Raw)*(l.Liters-pl.Liters)/float64(l.Raw-pl.Raw) + pl.Liters
								break
							}
						}
					}

					s.temp.Set(float64(temp))
					s.rawLevel.Set(float64(rawLevel))
					s.litersLevel.Set(level)
					s.errLineDamage.Set(float64(damageErr))
					s.errWater.Set(float64(waterErr))
				}

				break
			}
		}
	}()

	go func() {
		log.Info("write started")
		defer log.Info("write stopped")

		for {
			for _, s := range sensors {
				msg := []byte{0x31, byte(s.address), 0x06, 0}
				msg[3] = crc8(msg[:3])

				r := 0
				for r < len(msg) {
					select {
					case _ = <-e.stopChan:
						return
					default:
					}

					n, err := port.Write(msg[r:])
					if err != nil {
						e.onError(err)
						return
					}
					r += n
				}
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(config.ReadDelaySec))

			select {
			case _ = <-ctx.Done():
			case _ = <-e.stopChan:
				cancel()
				return
			}

			cancel()
		}
	}()

	outErr := <-e.errChan

	e.stopChan <- struct{}{}
	_ = port.Close()
	timeout, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	_ = httpServer.Shutdown(timeout)

	if outErr == stopError {
		return nil
	}

	return outErr
}

func (e *Exporter) onError(err error) {
	// process only one error
	select {
	case e.errChan <- err:
	default:
	}
}

func (e *Exporter) Shutdown() {
	e.onError(stopError)
}
