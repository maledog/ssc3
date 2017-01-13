// ssc3 project main.go
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/kardianos/osext"
	"github.com/maledog/logrot"
	"github.com/yosssi/gmq/mqtt"
	"github.com/yosssi/gmq/mqtt/client"
)

type Config struct {
	Log            Log            `json:"log"`
	Debug_mqtt     bool           `json:"debug_mqtt"`
	Debug_database bool           `json:"debug_database"`
	Debug_tcp      bool           `json:"debug_tcp"`
	Db             Sqlite_options `json:"db"`
	Channels       []Channel      `json:"channels"`
	Sensor_server  Sensor_server  `json:"sensor_server"`
	MQTT_options   MQTT_options   `json:"mqtt_options"`
}

type Sensor_server struct {
	Sending_interval_f string        `json:"sending_interval"`
	Sending_interval   time.Duration `json:"-"`
	Host               string        `json:"host"`
	Client_id          int           `json:"client_id"`
	Tcp_timeout_f      string        `json:"tcp_timeout"`
	Tcp_timeout        time.Duration `json:"-"`
	Row_limit          int           `json:"row_limit"`
	Min_row_interval_f string        `json:"min_row_interval"`
	Min_row_interval   time.Duration `json:"-"`
}

type Channel struct {
	Id                int           `json:"-"`
	Out_id            int           `json:"out_id"`
	Sub               string        `json:"subscribe"`
	Saving_interval_f string        `json:"saving_interval"`
	Saving_interval   time.Duration `json:"-"`
}

type Log struct {
	File      string `json:"file"`
	Max_size  int64  `json:"max_size"`
	Max_files int    `json:"max_files"`
}

type Sqlite_options struct {
	Db_file    string `json:"db_file"`
	Db_max_row int    `json:"db_max_row"`
}

type MQTT_options struct {
	Proto       string        `json:"proto"`
	Host        string        `json:"host"`
	User        string        `json:"user"`
	Password    string        `json:"password"`
	Client_id   string        `json:"client_id"`
	Reconnect_f string        `json:"reconnect"`
	Reconnect   time.Duration `json:"-"`
}

type Message struct {
	Topic string
	Value []byte
}

type MQTT_data struct {
	Topic string
	Value []byte
}

type Data_value struct {
	Id        int
	C_id      int
	Timestamp time.Time
	Value     float64
	Sent      bool
	Live      bool
}

func main() {
	exe_dir, err := osext.ExecutableFolder()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	var in_config string
	var mc bool
	var mkdb bool
	var dt bool
	var dl bool
	var dm bool
	flag.StringVar(&in_config, "c", exe_dir+"/config.json", "config file")
	flag.BoolVar(&mc, "m", false, "make config file")
	flag.BoolVar(&mkdb, "mkdb", false, "make new db_file")
	flag.BoolVar(&dt, "dt", false, "debug tcp_client stdout")
	flag.BoolVar(&dl, "dl", false, "debug database logger stdout")
	flag.BoolVar(&dm, "dm", false, "debug mqtt stdout")
	flag.Parse()
	if mc {
		cf, err := make_config(in_config)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		} else {
			log.Printf("Config file:%s\n", cf)
			os.Exit(0)
		}
	}
	cnf, err := get_config(in_config)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	if mkdb {
		err := init_db(cnf.Db.Db_file)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		} else {
			log.Printf("Init db_file:%s OK.\n", cnf.Db.Db_file)
			os.Exit(0)
		}
	}
	err = cnf.Parse_time()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	if dm || dl || dt {
		cnf.Log.File = ""
		if dm {
			cnf.Debug_mqtt = true
		}
		if dt {
			cnf.Debug_tcp = true
		}
		if dl {
			cnf.Debug_database = true
		}
	}
	if cnf.Log.File != "" {
		f, err := logrot.Open(cnf.Log.File, 0644, cnf.Log.Max_size, cnf.Log.Max_files)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
		defer f.Close()
		log.SetOutput(f)
	}
	db, err := open_sqlite(cnf.Db.Db_file)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	defer db.Close()
	ch_os_signals := make(chan os.Signal, 1)
	ch_mqtt_close := make(chan struct{}, 1)
	ch_worker_close := make(chan struct{}, 1)
	ch_db_logger_close := make(chan struct{}, 1)
	ch_sender_close := make(chan struct{}, 1)
	ch_arch_sender_close := make(chan struct{}, 1)
	ch_live_stop := make(chan bool, 1)
	signal.Notify(ch_os_signals, os.Interrupt, os.Kill, syscall.SIGTERM)
	ch_input := make(chan MQTT_data, 100)
	ch_in := make(chan Data_value, cnf.Sensor_server.Row_limit*2)
	ch_data := make(chan Data_value, cnf.Sensor_server.Row_limit*2)
	go db_logger(db, ch_data, ch_db_logger_close, cnf.Debug_database)
	go worker(cnf, ch_input, ch_data, ch_in, ch_live_stop, ch_worker_close)
	go tcp_client_wrapper(cnf, ch_in, ch_data, ch_sender_close)
	go arch_sender(db, cnf, ch_in, ch_live_stop, ch_arch_sender_close)
	go db_cleaner(db, cnf)
	go mqtt_client_wrapper(cnf, ch_input, ch_mqtt_close)

	<-ch_os_signals
	log.Println("Interrupted program.")
	ch_mqtt_close <- struct{}{}
	ch_worker_close <- struct{}{}
	ch_db_logger_close <- struct{}{}
	ch_arch_sender_close <- struct{}{}
	ch_sender_close <- struct{}{}
}

func mqtt_client_wrapper(cnf Config, ch_input chan MQTT_data, ch_mqtt_close chan struct{}) {
	ch_mqtt_client_close := make(chan struct{}, 1)
	for {
		select {
		case <-ch_mqtt_close:
			ch_mqtt_client_close <- struct{}{}
			break
		default:
			{
				err := mqtt_client(cnf, ch_input, ch_mqtt_client_close)
				if err != nil {
					log.Printf("MQTT:ERROR:%v.\n", err)
				}
				log.Printf("MQTT:Next connect in %s.\n", cnf.MQTT_options.Reconnect.String())
				time.Sleep(cnf.MQTT_options.Reconnect)
			}
		}
	}
}

func mqtt_client(cnf Config, ch_input chan MQTT_data, ch_mqtt_client_close chan struct{}) error {
	ch_mqtt_error := make(chan error, 1)
	var opts client.Options
	opts.ErrorHandler = func(err error) {
		ch_mqtt_error <- err
	}
	var conn_opts client.ConnectOptions
	conn_opts.Network = cnf.MQTT_options.Proto
	conn_opts.Address = cnf.MQTT_options.Host
	conn_opts.ClientID = []byte(cnf.MQTT_options.Client_id)
	conn_opts.CleanSession = true
	conn_opts.UserName = []byte(cnf.MQTT_options.User)
	conn_opts.Password = []byte(cnf.MQTT_options.Password)
	conn_opts.KeepAlive = 30
	var sr client.SubscribeOptions
	for i := 0; i < len(cnf.Channels); i++ {
		topic := cnf.Channels[i].Sub
		log.Printf("MQTT:Subscribe to %q.\n", topic)
		var r client.SubReq
		r.TopicFilter = []byte(topic)
		r.QoS = mqtt.QoS0
		r.Handler = func(topic, message []byte) {
			var in_mess MQTT_data
			in_mess.Topic = string(topic)
			in_mess.Value = message
			if cnf.Debug_mqtt {
				log.Printf("MQTT:DEBUG:%q %q.\n", in_mess.Topic, string(message))
			}
			select {
			case ch_input <- in_mess:
			default:
			}
		}
		sr.SubReqs = append(sr.SubReqs, &r)
	}
	cli := client.New(&opts)
	err := cli.Connect(&conn_opts)
	if err != nil {
		return err
	}
	log.Printf("MQTT:Connected to %s.\n", cnf.MQTT_options.Host)
	defer cli.Terminate()
	err = cli.Subscribe(&sr)
	if err != nil {
		return err
	}
	select {
	case <-ch_mqtt_client_close:
		err := cli.Disconnect()
		log.Printf("MQTT:Client disconnected.\n")
		return err
	case err := <-ch_mqtt_error:
		return err
	}
}

func worker(cnf Config, ch_input chan MQTT_data, ch_data chan Data_value, ch_in chan Data_value, ch_live_stop chan bool, ch_worker_close chan struct{}) {
	live_stop := false
	radiator_arch := make(map[string]time.Time)
	radiator_live := make(map[string]time.Time)
	live_interval := time.Duration(len(cnf.Channels)) * cnf.Sensor_server.Min_row_interval
	ch_ids := make(map[string]int)
	ch_intervals := make(map[string]time.Duration)
	for _, channel := range cnf.Channels {
		radiator_arch[channel.Sub] = time.Now().Add(-1*channel.Saving_interval + 5*time.Second)
		radiator_live[channel.Sub] = time.Now().Add(-1*live_interval + 1*time.Second)
		ch_ids[channel.Sub] = channel.Id
		ch_intervals[channel.Sub] = channel.Saving_interval
	}
	for {
		select {
		case <-ch_worker_close:
			{
				break
			}
		default:
			{
				in_mess := <-ch_input
				val, err := strconv.ParseFloat(string(in_mess.Value), 64)
				var data Data_value
				data.Timestamp = time.Now()
				data.C_id = ch_ids[in_mess.Topic]
				data.Value = val
				data.Live = true
				if err != nil {
					log.Println(err)
					continue
				}
				if !time.Now().Before(radiator_arch[in_mess.Topic].Add(ch_intervals[in_mess.Topic])) {
					select {
					case ch_data <- data:
						{
							radiator_arch[in_mess.Topic] = time.Now()
						}
					default:
					}
				}
				if !time.Now().Before(radiator_live[in_mess.Topic].Add(live_interval)) {
					select {
					case live_stop = <-ch_live_stop:
						{
							radiator_live[in_mess.Topic] = time.Now()
						}
					default:
					}
					if !live_stop {
						select {
						case ch_in <- data:
							{
								radiator_live[in_mess.Topic] = time.Now()
							}
						default:
						}
					}
				}
			}
		}
	}
	return
}

func tcp_client_wrapper(cnf Config, ch_in chan Data_value, ch_data chan Data_value, ch_sender_close chan struct{}) {
	ch_tcp_client_close := make(chan struct{}, 1)
	for {
		select {
		case <-ch_sender_close:
			ch_tcp_client_close <- struct{}{}
			break
		default:
			{
				err := tcp_client(cnf, ch_in, ch_data, ch_tcp_client_close)
				if err != nil {
					log.Printf("TCP:%v.\n", err)
					log.Printf("TCP:Client disconnected.\n")
					log.Printf("TCP:Next connect in 30 second.\n")
					time.Sleep(30 * time.Second)
				}
			}
		}
	}
}

func tcp_client(cnf Config, ch_in chan Data_value, ch_data chan Data_value, ch_tcp_client_close chan struct{}) error {
	chs := make(map[int]int)
	for _, channel := range cnf.Channels {
		chs[channel.Id] = channel.Out_id
	}
	log.Printf("SENSER: Connecting to %s\n", cnf.Sensor_server.Host)
	conn, err := net.DialTimeout("tcp4", cnf.Sensor_server.Host, cnf.Sensor_server.Tcp_timeout)
	if err != nil {
		return err
	}
	log.Printf("SENSER: The connection to %s established.\n", cnf.Sensor_server.Host)
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	for {
		select {
		case <-ch_tcp_client_close:
			{
				return nil
			}
		default:
			{
				data := <-ch_in
				var out_str string
				if data.Live {
					out_str = fmt.Sprintf("#id:%d;datetime:%s;saving_interval:%d;live:1;t%d:%.2f#", cnf.Sensor_server.Client_id, data.Timestamp.UTC().Format("20060102150405"), int(cnf.Sensor_server.Sending_interval.Seconds()), chs[data.C_id], to_fixed(data.Value, 2))
				} else {
					out_str = fmt.Sprintf("#id:%d;datetime:%s;saving_interval:%d;live:0;t%d:%.2f#", cnf.Sensor_server.Client_id, data.Timestamp.UTC().Format("20060102150405"), int(cnf.Sensor_server.Sending_interval.Seconds()), chs[data.C_id], to_fixed(data.Value, 2))
				}
				conn.SetDeadline(time.Now().Add(cnf.Sensor_server.Tcp_timeout))
				resp, err := send_sensor(reader, writer, out_str)
				if err != nil {
					return err
				} else {
					switch resp {
					case "@OK\r\n":
						{
							if !data.Live {
								select {
								case ch_data <- data:
								default:
								}
							}
						}
					case "@FAIL\r\n":
					default:
						log.Println("unknown resp:", resp)
					}
					if cnf.Debug_tcp {
						debug_str := strings.Replace(strings.Replace(resp, "\r", "<CR>", -1), "\n", "<LF>", -1)
						if data.Live {
							log.Printf("SENDER_LIVE: %s, response:%q\n", out_str, debug_str)
						} else {
							log.Printf("SENDER_ARCH: %s, response:%q\n", out_str, debug_str)
						}
					}
				}
				time.Sleep(cnf.Sensor_server.Min_row_interval)
			}
		}
	}
	return nil
}

func send_sensor(reader *bufio.Reader, writer *bufio.Writer, out_str string) (string, error) {
	_, err := writer.Write([]byte(out_str + "\n"))
	if err != nil {
		return "", err
	}
	err = writer.Flush()
	if err != nil {
		return "", err
	}
	buf := make([]byte, 10)
	n, err := reader.Read(buf)
	return string(buf[:n]), err
}

func get_config(cf string) (cnf Config, err error) {
	data, err := ioutil.ReadFile(cf)
	if err != nil {
		return
	}
	err = json.Unmarshal(data, &cnf)
	return
}

func make_config(cf string) (string, error) {
	var cnf Config
	var channel Channel
	cnf.Log.File = "logs/ssc3.log"
	cnf.Log.Max_size = 10 * 1024 * 1024
	cnf.Log.Max_files = 10
	cnf.Debug_mqtt = false
	cnf.Debug_database = false
	cnf.Debug_tcp = false
	cnf.Db.Db_file = "db/ssc3.sqlite"
	cnf.Db.Db_max_row = 100000
	cnf.Sensor_server.Host = "127.0.0.1:7115"
	cnf.Sensor_server.Client_id = 0
	cnf.Sensor_server.Sending_interval_f = "300s"
	cnf.Sensor_server.Row_limit = 500
	cnf.Sensor_server.Min_row_interval_f = "500ms"
	cnf.Sensor_server.Tcp_timeout_f = "10s"
	cnf.MQTT_options.Proto = "tcp"
	cnf.MQTT_options.Host = "localhost:1883"
	cnf.MQTT_options.User = ""
	cnf.MQTT_options.Password = ""
	cnf.MQTT_options.Client_id = "ssc3"
	cnf.MQTT_options.Reconnect_f = "30s"
	channel.Out_id = 1
	channel.Sub = "/devices/device_id/controls/control_id_1"
	channel.Saving_interval_f = "60s"
	cnf.Channels = append(cnf.Channels, channel)
	channel.Out_id = 2
	channel.Sub = "/devices/device_id/controls/control_id_2"
	channel.Saving_interval_f = "60s"
	cnf.Channels = append(cnf.Channels, channel)
	data, err := json.MarshalIndent(cnf, "", "\t")
	if err != nil {
		return "", err
	}
	err = ioutil.WriteFile(cf, data, 0644)
	if err != nil {
		return "", err
	}
	return cf, nil
}

func fso_exist(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		return false
	}
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}

func to_fixed(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return float64(round(num*output)) / output
}
