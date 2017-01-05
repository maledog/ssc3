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

	"github.com/maledog/logrot"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/kardianos/osext"
)

type F_Config struct {
	Log           string          `json:"log"`
	Dd            Sqlite_options  `json:"db"`
	Channels      []F_Channel     `json:"channels"`
	Sensor_server F_Sensor_server `json:"sensor_server"`
	MQTT_options  MQTT_options    `json:"mqtt_options"`
}

type F_Sensor_server struct {
	Debug               bool   `json:"debug"`
	Sending_interval    int    `json:"sending_interval"`
	Client_id           int    `json:"client_id"`
	Host                string `json:"host"`
	Tcp_timeout         int    `json:"tcp_timeout"`
	Row_limit           int    `json:"row_limit"`
	Min_row_interval_ms int    `json:"min_row_interval_ms"`
}

type F_Channel struct {
	Id              int    `json:"-"`
	Out_id          int    `json:"out_id"`
	Sub             string `json:"subscribe"`
	Saving_interval int    `json:"saving_interval"`
}

type Config struct {
	Log           string
	Db            Sqlite_options
	Channels      []Channel
	Sensor_server Sensor_server
	MQTT_options  MQTT_options
}

type Sensor_server struct {
	Debug               bool
	Sending_interval    time.Duration
	Client_id           int
	Host                string
	Tcp_timeout         time.Duration
	Row_limit           int
	Min_row_interval_ms time.Duration
}

type Channel struct {
	Id              int
	Out_id          int
	Sub             string
	Saving_interval time.Duration
}

type Sqlite_options struct {
	Debug      bool   `json:"debug"`
	Db_file    string `json:"db_file"`
	Db_max_row int    `json:"db_max_row"`
}

type MQTT_options struct {
	Debug    bool   `json:"debug"`
	Host     string `json:"host"`
	User     string `json:"user"`
	Password string `json:"password"`
}

type Message struct {
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
	var make_config bool
	var mkdb bool
	var ds bool
	var dl bool
	var dm bool
	flag.StringVar(&in_config, "c", exe_dir+"/config.json", "config file")
	flag.BoolVar(&make_config, "m", false, "make config file")
	flag.BoolVar(&mkdb, "mkdb", false, "make new db_file")
	flag.BoolVar(&ds, "ds", false, "debug sender stdout")
	flag.BoolVar(&dl, "dl", false, "debug database logger stdout")
	flag.BoolVar(&dm, "dm", false, "debug mqtt stdout")
	flag.Parse()
	if make_config {
		cf, err := make_f_config(in_config)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		} else {
			log.Printf("Config file:%s\n", cf)
			os.Exit(0)
		}
	}
	f_cnf, err := get_f_config(in_config)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	if mkdb {
		err := init_db(f_cnf.Dd.Db_file)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		} else {
			log.Printf("Init db_file:%s OK.\n", f_cnf.Dd.Db_file)
			os.Exit(0)
		}
	}
	cnf, err := get_config(f_cnf)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	if dm || dl || ds {
		cnf.Log = ""
		if dm {
			cnf.MQTT_options.Debug = true
		}
		if ds {
			cnf.Sensor_server.Debug = true
		}
		if dl {
			cnf.Db.Debug = true
		}
	}
	if cnf.Log != "" {
		f, err := logrot.Open(cnf.Log, 0644, 10*1024*1024, 10)
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
	ch_mqtt_error := make(chan bool, 1)
	ch_live_stop := make(chan bool, 1)
	signal.Notify(ch_os_signals, os.Interrupt, os.Kill, syscall.SIGTERM)
	ch_input := make(chan Message, 100)
	ch_in := make(chan Data_value, cnf.Sensor_server.Row_limit*2)
	ch_data := make(chan Data_value, cnf.Sensor_server.Row_limit*2)
	go db_logger(db, ch_data, ch_db_logger_close, cnf.Db.Debug)
	go worker(cnf, ch_input, ch_data, ch_in, ch_live_stop, ch_worker_close)
	go tcp_client_wrapper(cnf, ch_in, ch_data, ch_sender_close)
	go arch_sender(db, cnf, ch_in, ch_live_stop, ch_arch_sender_close)
	go db_cleaner(db, cnf)
	connected := false
	for !connected {
		go start_mqtt_client(cnf, ch_input, ch_mqtt_close, ch_mqtt_error)
		connected = <-ch_mqtt_error
		if !connected {
			ch_mqtt_close <- struct{}{}
			time.Sleep(1 * time.Minute)
		}
	}

	<-ch_os_signals
	log.Println("Interrupted program.")
	ch_mqtt_close <- struct{}{}
	ch_worker_close <- struct{}{}
	ch_db_logger_close <- struct{}{}
	ch_arch_sender_close <- struct{}{}
	ch_sender_close <- struct{}{}
}

func start_mqtt_client(cnf Config, ch_input chan Message, ch_mqtt_close chan struct{}, ch_mqtt_error chan bool) {
	var conn_opts MQTT.ClientOptions
	conn_opts.AddBroker(cnf.MQTT_options.Host)
	conn_opts.SetUsername(cnf.MQTT_options.User)
	conn_opts.SetPassword(cnf.MQTT_options.Password)
	conn_opts.SetClientID("sensor_mqtt_serial")
	conn_opts.SetCleanSession(true)
	conn_opts.SetAutoReconnect(true)
	conn_opts.SetMaxReconnectInterval(1 * time.Minute)
	//conn_opts.SetKeepAlive(30 * time.Second)
	conn_opts.OnConnect = func(c MQTT.Client) {
		for i := 0; i < len(cnf.Channels); i++ {
			topic := cnf.Channels[i].Sub
			log.Printf("Subscribe to topic:%s\n", topic)
			if token := c.Subscribe(topic, byte(0), func(client MQTT.Client, message MQTT.Message) {
				var in_mess Message
				in_mess.Topic = message.Topic()
				in_mess.Value = message.Payload()
				if cnf.MQTT_options.Debug {
					log.Println("MQTT:", message.Topic(), string(message.Payload()))
				}
				select {
				case ch_input <- in_mess:
				default:
				}
			}); token.Wait() && token.Error() != nil {
				log.Println("Mqtt subscribe error:", token.Error())
			}
		}
	}
	conn_opts.OnConnectionLost = func(c MQTT.Client, err error) {
		log.Println("Mqtt connection lost, error:", err)
	}
	client := MQTT.NewClient(&conn_opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Println("Mqtt connection error:", token.Error())
		ch_mqtt_error <- false
	} else {
		log.Printf("Connected to %s\n", cnf.MQTT_options.Host)
		ch_mqtt_error <- true
	}

	defer func() {
		if client.IsConnected() {
			client.Disconnect(1)
		}
	}()
	<-ch_mqtt_close
	return
}

func worker(cnf Config, ch_input chan Message, ch_data chan Data_value, ch_in chan Data_value, ch_live_stop chan bool, ch_worker_close chan struct{}) {
	live_stop := false
	radiator_arch := make(map[string]time.Time)
	radiator_live := make(map[string]time.Time)
	live_interval := time.Duration(len(cnf.Channels)) * cnf.Sensor_server.Min_row_interval_ms
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
					if cnf.Sensor_server.Debug {
						debug_str := strings.Replace(strings.Replace(resp, "\r", "<CR>", -1), "\n", "<LF>", -1)
						if data.Live {
							log.Printf("SENDER_LIVE: %s, response:%q\n", out_str, debug_str)
						} else {
							log.Printf("SENDER_ARCH: %s, response:%q\n", out_str, debug_str)
						}
					}
				}
				time.Sleep(cnf.Sensor_server.Min_row_interval_ms)
			}
		}
	}
	return nil
}

func sender(cnf Config, ch_in chan Data_value, ch_data chan Data_value, ch_sender_close chan struct{}) {
	chs := make(map[int]int)
	for _, channel := range cnf.Channels {
		chs[channel.Id] = channel.Out_id
	}
	for {
		select {
		case <-ch_sender_close:
			{
				break
			}
		default:
			{
				log.Printf("SENSER: Connecting to %s\n", cnf.Sensor_server.Host)
				conn, err := net.DialTimeout("tcp4", cnf.Sensor_server.Host, cnf.Sensor_server.Tcp_timeout)
				if err != nil {
					log.Println(err)
					break
				}
				defer conn.Close()
				reader := bufio.NewReader(conn)
				writer := bufio.NewWriter(conn)
				connected := true
				for connected {
					select {
					case <-ch_sender_close:
						{
							break
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
							resp, err := send_sensor(reader, writer, out_str)
							if err != nil {
								log.Println(err)
								connected = false
								break
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
								if cnf.Sensor_server.Debug {
									debug_str := strings.Replace(strings.Replace(resp, "\r", "<CR>", -1), "\n", "<LF>", -1)
									if data.Live {
										log.Printf("SENDER_LIVE: %s, response:%q\n", out_str, debug_str)
									} else {
										log.Printf("SENDER_ARCH: %s, response:%q\n", out_str, debug_str)
									}
								}
							}
							time.Sleep(cnf.Sensor_server.Min_row_interval_ms)
						}
					}
				}
				log.Println("Sender disconnect.")
				conn.Close()
			}
		}
		time.Sleep(15 * time.Second)
	}
	return
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

func get_f_config(cf string) (cnf F_Config, err error) {
	data, err := ioutil.ReadFile(cf)
	if err != nil {
		return
	}
	err = json.Unmarshal(data, &cnf)
	return
}

func make_f_config(cf string) (string, error) {
	var cnf F_Config
	var channel F_Channel
	cnf.Log = "logs/ssc3.log"
	cnf.Dd.Debug = false
	cnf.Dd.Db_file = "db/ssc3.sqlite"
	cnf.Dd.Db_max_row = 100000
	cnf.Sensor_server.Debug = false
	cnf.Sensor_server.Host = "127.0.0.1:7115"
	cnf.Sensor_server.Client_id = 0
	cnf.Sensor_server.Sending_interval = 300
	cnf.Sensor_server.Row_limit = 500
	cnf.Sensor_server.Min_row_interval_ms = 1000
	cnf.Sensor_server.Tcp_timeout = 10
	cnf.MQTT_options.Debug = false
	cnf.MQTT_options.Host = "tcp://localhost:1883"
	cnf.MQTT_options.User = ""
	cnf.MQTT_options.Password = ""
	channel.Out_id = 1
	channel.Sub = "/devices/device_id/controls/control_id_1"
	channel.Saving_interval = 60
	cnf.Channels = append(cnf.Channels, channel)
	channel.Out_id = 2
	channel.Sub = "/devices/device_id/controls/control_id_2"
	channel.Saving_interval = 60
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
