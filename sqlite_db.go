// ssc3 project sqlite_db.go
package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func init_db(db_file string) error {
	if fso_exist(db_file) {
		backup_file := db_file + ".backup"
		err := os.Rename(db_file, backup_file)
		if err != nil {
			return err
		}
	}
	db, err := sql.Open("sqlite3", db_file)
	if err != nil {
		return err
	}
	defer db.Close()

	create_table := `DROP TABLE IF EXISTS "data";
	CREATE TABLE data ("id" INTEGER PRIMARY KEY  NOT NULL, "c_id" INTEGER DEFAULT 0, "timestamp" DEFAULT (strftime('%s', 'now')),"value" REAL, "sent" INTEGER DEFAULT 0);
	DROP TABLE IF EXISTS "channels";
	CREATE TABLE channels ("id" INTEGER PRIMARY KEY  NOT NULL, "out_id" INTEGER DEFAULT 0, "subscribe" TEXT);
	INSERT INTO channels VALUES (0, 0, "Unknown");
	`
	_, err = db.Exec(create_table)
	if err != nil {
		return err
	}
	return nil
}

func get_config(f_cnf F_Config) (cnf Config, err error) {
	cnf.Log = f_cnf.Log
	cnf.MQTT_options = f_cnf.MQTT_options
	cnf.Db = f_cnf.Dd
	cnf.Sensor_server.Debug = f_cnf.Sensor_server.Debug
	cnf.Sensor_server.Client_id = f_cnf.Sensor_server.Client_id
	cnf.Sensor_server.Host = f_cnf.Sensor_server.Host
	cnf.Sensor_server.Row_limit = f_cnf.Sensor_server.Row_limit
	cnf.Sensor_server.Min_row_interval_ms = time.Duration(f_cnf.Sensor_server.Min_row_interval_ms) * time.Millisecond
	cnf.Sensor_server.Sending_interval = time.Duration(f_cnf.Sensor_server.Sending_interval) * time.Second
	cnf.Sensor_server.Tcp_timeout = time.Duration(f_cnf.Sensor_server.Tcp_timeout) * time.Second
	db, err := sql.Open("sqlite3", cnf.Db.Db_file)
	if err != nil {
		return
	}
	for _, f_ch := range f_cnf.Channels {
		var ch Channel
		ch.Id = f_ch.Id
		ch.Sub = f_ch.Sub
		ch.Out_id = f_ch.Out_id
		ch.Saving_interval = time.Duration(f_ch.Saving_interval) * time.Second
		var id int64
		sql_query := "SELECT id FROM channels WHERE subscribe=?;"
		err = db.QueryRow(sql_query, ch.Sub).Scan(&id)
		if err != nil {
			sql_query := "INSERT INTO channels(out_id,subscribe) VALUES (?,?);"
			res, err := db.Exec(sql_query, ch.Out_id, ch.Sub)
			if err != nil {
				return cnf, err
			}
			id, err = res.LastInsertId()
			if err != nil {
				return cnf, err
			}
		}
		ch.Id = int(id)
		cnf.Channels = append(cnf.Channels, ch)
	}
	return cnf, nil
}

func open_sqlite(db_file string) (*sql.DB, error) {
	return sql.Open("sqlite3", db_file)
}

func db_logger(db *sql.DB, ch_data chan Data_value, ch_db_logger_close chan struct{}, debug bool) {
	var updates []int
	for {
		select {
		case <-ch_db_logger_close:
			{
				break
			}
		default:
			{
				data := <-ch_data
				if data.Live {
					if debug {
						log.Printf("DB_LOGGER_LIVE: time:%s, channel:%d, value:%f\n", data.Timestamp.Format("2006-01-02 15:04:05"), data.C_id, data.Value)
					}
					sql_query := "INSERT INTO data(c_id,timestamp,value) VALUES (?,?,?);"
					_, err := db.Exec(sql_query, data.C_id, data.Timestamp.Unix(), data.Value)
					if err != nil {
						log.Println(err)
						continue
					}
				} else {
					if debug {
						log.Printf("DB_LOGGER_ARCH: time:%s, id:%d\n", data.Timestamp.Format("2006-01-02 15:04:05"), data.Id)
					}
					if len(updates) < 100 {
						updates = append(updates, data.Id)
						continue
					} else {
						tx, err := db.Begin()
						if err != nil {
							log.Println(err)
							continue
						}
						stmt, err := tx.Prepare("UPDATE data SET sent = 1 WHERE id = ?;")
						if err != nil {
							log.Println(err)
							continue
						}
						defer stmt.Close()
						for _, id := range updates {
							_, err := stmt.Exec(id)
							if err != nil {
								stmt.Close()
								log.Println(err)
								break
							}
						}
						updates = []int{}
						err = tx.Commit()
						if err != nil {
							log.Println(err)
							continue
						}
					}
				}
			}
		}

	}
	return
}

func arch_sender(db *sql.DB, cnf Config, ch_in chan Data_value, ch_live_stop chan bool, ch_arch_sender_close chan struct{}) {
	ticker := time.NewTicker(cnf.Sensor_server.Sending_interval)
	chs := make(map[int]int)
	for _, channel := range cnf.Channels {
		chs[channel.Id] = channel.Out_id
	}
	for {
		select {
		case <-ch_arch_sender_close:
			{
				ticker.Stop()
				break
			}
		default:
			{
				<-ticker.C
				select {
				case ch_live_stop <- true:
					{
						time.Sleep(time.Duration(len(cnf.Channels)) * cnf.Sensor_server.Min_row_interval_ms)
					}
				default:
				}
				sql_query := "SELECT id,c_id,timestamp,value FROM data WHERE sent=0 LIMIT ?;"
				rows, err := db.Query(sql_query, cnf.Sensor_server.Row_limit)
				if err != nil {
					log.Println(err)
					continue
				}
				defer rows.Close()
				var out_data []Data_value
				for rows.Next() {
					var id int
					var c_id int
					var timestamp int64
					var value float64
					var out Data_value
					out.Live = false
					err = rows.Scan(&id, &c_id, &timestamp, &value)
					out.Id = id
					out.C_id = c_id
					out.Timestamp = time.Unix(timestamp, 0)
					out.Value = value
					out_data = append(out_data, out)
				}
				for _, out := range out_data {
					select {
					case ch_in <- out:
						{
							time.Sleep(time.Duration(2) * cnf.Sensor_server.Min_row_interval_ms)
						}
					default:
					}
				}
				time.Sleep(time.Duration(len(out_data)*2) * cnf.Sensor_server.Min_row_interval_ms)
				select {
				case ch_live_stop <- false:
					{
					}
				default:
				}
			}
		}
	}
	return
}

func db_cleaner(db *sql.DB, cnf Config) {
	ticker := time.NewTicker(10 * time.Minute)
	for {
		<-ticker.C
		_, err := db.Exec("DELETE FROM data WHERE id NOT IN (SELECT id FROM data ORDER BY id DESC LIMIT ?);", cnf.Db.Db_max_row)
		if err != nil {
			log.Println(err)
			continue
		}
	}
	return
}
