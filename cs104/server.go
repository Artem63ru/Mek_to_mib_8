// Copyright 2020 thinkgos (thinkgo@aliyun.com).  All rights reserved.
// Use of this source code is governed by a version 3 of the GNU General
// Public License, license that can be found in the LICENSE file.

package cs104

import (
	"context"
	"crypto/tls"
	"database/sql"
	_ "database/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/thinkgos/go-iecp5/asdu"
	"github.com/thinkgos/go-iecp5/clog"
	"net"
	"sync"
	"time"
)

// timeoutResolution is seconds according to companion standard 104,
// subclass 6.9, caption "Definition of time outs". However, then
// of a second make this system much more responsive i.c.w. S-frames.
const timeoutResolution = 100 * time.Millisecond

// Путь к БД
const dbpath = "test.db"

// Server the common server
type Server struct {
	config         Config
	params         asdu.Params
	handler        ServerHandlerInterface
	TLSConfig      *tls.Config
	mux            sync.Mutex
	sessions       map[*SrvSession]struct{}
	listen         net.Listener
	onConnection   func(asdu.Connect)
	connectionLost func(asdu.Connect)
	clog.Clog
	wg sync.WaitGroup
}

// Структура таблицы БД SQLite
type Paramerts struct {
	Id     string
	Addres int       //Адресс в АСДУ
	Value  float32   //Значение
	QDS    int       //Качество
	Date   time.Time //Метка времени
}

// NewServer new a server, default config and default asdu.ParamsWide params
func NewServer(handler ServerHandlerInterface) *Server {
	return &Server{
		config:   DefaultConfig(),
		params:   *asdu.ParamsWide,
		handler:  handler,
		sessions: make(map[*SrvSession]struct{}),
		Clog:     clog.NewLogger("cs104 server => "),
	}
}

// SetConfig set config if config is valid it will use DefaultConfig()
func (sf *Server) SetConfig(cfg Config) *Server {
	if err := cfg.Valid(); err != nil {
		sf.config = DefaultConfig()
	} else {
		sf.config = cfg
	}
	return sf
}

// SetParams set asdu params if params is valid it will use asdu.ParamsWide
func (sf *Server) SetParams(p *asdu.Params) *Server {
	if err := p.Valid(); err != nil {
		sf.params = *asdu.ParamsWide
	} else {
		sf.params = *p
	}
	return sf
}

// ListenAndServer run the server
func (sf *Server) ListenAndServer(addr string) {
	//var connect = false
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		sf.Error("server run failed, %v", err)
		return
	}
	sf.mux.Lock()
	sf.listen = listen
	sf.mux.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		_ = sf.Close()
		sf.Debug("server stop")
	}()
	sf.Debug("server run")
	// Работа с БД
	//db := InitDB(dbpath)
	//defer db.Close()

	//go func() {
	//	var val_par [][]asdu.BD_params_float
	//	for {
	//		asdu.Check_value()
	//		time.Sleep(time.Second * 1)
	//		if connect {
	//			sf.Debug("UpDate Value in connect")
	//			if len(val_par) > 0 {
	//				for i := 0; i < len(val_par); i++ {
	//					asdu.Transfer_buff(sf, val_par[i])
	//					//time.Sleep(time.Second * 1)
	//				}
	//				val_par = nil
	//			}
	//		} else {
	//			sf.Debug("UpDate Value in no connection")
	//
	//			par := []asdu.BD_params_float{asdu.Par_send[0], asdu.Par_send[1], asdu.Par_send[2], asdu.Par_send[3]}
	//			val_par = append(val_par, par)
	//			time.Sleep(time.Second * 5)
	//
	//		}
	//
	//	}
	//}()
	for {
		conn, err := listen.Accept()
		if err != nil {
			sf.Error("server run failed, %v", err)
			return
		}

		sf.wg.Add(1)
		go func() {
			//	connect = true
			sess := &SrvSession{
				config:   &sf.config,
				params:   &sf.params,
				handler:  sf.handler,
				conn:     conn,
				rcvASDU:  make(chan []byte, sf.config.RecvUnAckLimitW<<4),
				sendASDU: make(chan []byte, sf.config.SendUnAckLimitK<<4),
				rcvRaw:   make(chan []byte, sf.config.RecvUnAckLimitW<<5),
				sendRaw:  make(chan []byte, sf.config.SendUnAckLimitK<<5), // may not block!

				onConnection:   sf.onConnection,
				connectionLost: sf.connectionLost,
				Clog:           sf.Clog,
			}

			sf.mux.Lock()
			sf.sessions[sess] = struct{}{}
			sf.mux.Unlock()
			sess.run(ctx)
			sf.mux.Lock()
			delete(sf.sessions, sess)
			sf.mux.Unlock()
			sf.wg.Done()
			//connect = false
		}()
	}
}

// Close close the server
func (sf *Server) Close() error {
	var err error

	sf.mux.Lock()
	if sf.listen != nil {
		err = sf.listen.Close()
		sf.listen = nil
	}
	sf.mux.Unlock()
	sf.wg.Wait()
	return err
}

// Send imp interface Connect
func (sf *Server) Send(a *asdu.ASDU) error {
	sf.mux.Lock()
	for k := range sf.sessions {
		_ = k.Send(a.Clone())
	}
	sf.mux.Unlock()
	return nil
}

// Params imp interface Connect
func (sf *Server) Params() *asdu.Params { return &sf.params }

// UnderlyingConn imp interface Connect
func (sf *Server) UnderlyingConn() net.Conn { return nil }

// SetInfoObjTimeZone set info object time zone
func (sf *Server) SetInfoObjTimeZone(zone *time.Location) {
	sf.params.InfoObjTimeZone = zone
}

// SetOnConnectionHandler set on connect handler
func (sf *Server) SetOnConnectionHandler(f func(asdu.Connect)) {
	sf.onConnection = f
}

// SetConnectionLostHandler set connect lost handler
func (sf *Server) SetConnectionLostHandler(f func(asdu.Connect)) {
	sf.connectionLost = f
}

// Инициализация подключения к БД
func InitDB(filepath string) *sql.DB {
	db, err := sql.Open("sqlite3", filepath)
	if err != nil {
		panic(err)
	}
	if db == nil {
		panic("db nil")
	}
	return db
}

// Запись данных в БД
func StoreItem(db *sql.DB, items Paramerts) {

	_, err := db.Exec("insert into params (Addres, QDS, date, Value) values ($1, $2, $3, $4)",
		items.Addres, items.QDS, items.Date.String(), items.Value)
	if err != nil {
		panic(err)
	}
	return

}
