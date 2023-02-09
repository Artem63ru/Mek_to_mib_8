// Copyright 2020 thinkgos (thinkgo@aliyun.com).  All rights reserved.
// Use of this source code is governed by a version 3 of the GNU General
// Public License, license that can be found in the LICENSE file.

package cs104

import (
	"MEK104/asdu"
	"MEK104/clog"
	modbus_mk "MEK104/modbus"
	"context"
	"crypto/tls"
	"database/sql"
	_ "database/sql"
	"fmt"
	"github.com/BurntSushi/toml"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

// timeoutResolution is seconds according to companion standard 104,
// subclass 6.9, caption "Definition of time outs". However, then
// of a second make this system much more responsive i.c.w. S-frames.
const timeoutResolution = 100 * time.Millisecond

var ConfigT TomlConfig               // структура конфигуратора
var Log *log.Logger                  // это логер для записи
var Buff [100]asdu.BD_params_float   // массив аналоговых параметров
var Buff_D [100]asdu.BD_params_singl // массив параметров
var Buff_KR [100]asdu.BD_params_KR   // массив параметров кранов
var Count_Anpar int
var Count_DIpar int
var Count_DOpar int
var Сonnect_srv = false

// Массив параметров флоат для передачи параметров по МЭК 104.
var Par_send []asdu.BD_params_float

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

// Структура таблицы БД SQLite / и в файле конфигурации
type Paramerts struct {
	Id     uint16
	Addres uint16    //Адресс в АСДУ
	Hi     string    // верхняя граница
	Low    string    // нижняя граница
	Value  uint16    //Значение
	QDS    uint16    //Качество
	Date   time.Time //Метка времени

}

// структура конфигурации
type TomlConfig struct {
	Version          string       // версия
	Upport_tcp       uint16       // порт для запросов верзнего уровня
	Local_port       uint16       // локальный порт для работы с управляющей программой
	Tty_serial       []Set_serial // миссив настроек для устройств по tty
	Tcp_serial       []Set_tcp    // массив каналов по TCP
	Count_tty_serial int          // количество используемых каналов tty
	Count_tcp_serial int          // еолизество используемых каналов tcp
}

// описание канала ро TCP
type Set_tcp struct {
	Ip         string // IP адрес канала
	Port       int    // порт для реализации сервера
	Count_node uint   // количество устройств на порту
	Set_node   []Node // это массив нод
	Time_loop  uint   // время цикла опроса master между нодами
}

// описание ноды на канале TCP
type Node struct {
	Address_id   uint8       // адрес на шине
	Enable       bool        // включено устройство в опрос или нет
	Command      uint8       // обрабатываемая комманда опроса
	Address_data uint16      // адрес начала данных с ноде
	Data_length  uint16      // длинна данных
	Index_up     uint        // позиция данных с ноды в на глобальной карте параметров
	Type_par     uint        // Тип параметра получаемого от такт-у 1-AI, 2-DI
	Params       []Paramerts // Описание каждого параметра
	// изменяется во время опроса по ответу-неответу от устройства - делать не здесь, а в статусе
	//	Time   time.Time // время последнего опроса
	//	Status uint8     // статус опроса устройства

}

// настройки канала для всех нод однотипные
type Set_serial struct {
	Port_tty string // последовательный порт
	Baud     uint   // скорость обменеа
	Stop     uint8  // количество стопов
	Bits     uint8  // количество бит
	Parity   string // паритет "N"-нет,"E"-event,"O"-odd
	Slave    bool   // канал slave: true - slave - выдача запросов из макипоранныой катру параметров
	// если true - то параметры ниже не работают
	Count_node uint   // количество устройств на порту
	Set_node   []Node // это массив нод
	Time_loop  uint   // время цикла опроса master между нодами
}

// ***************************************************************************************
// проверка на корректность прараметров конфигурации, считанной из toml файла
// возвращает true, если успешно
func config_control() bool {

	return true
}

// создание переменных
func Ser_Init(path string) {
	Count_Anpar = 0
	Count_DOpar = 0
	Count_DIpar = 0
	// далее обработка toml файла конфигурации
	_, errt := toml.DecodeFile(path, &ConfigT)
	if errt != nil {
		Log.Println("Eror load *.toml file")
		Log.Println(errt)
		os.Exit(-1)
	}

	fmt.Printf("version toml: %s\n", ConfigT.Version)
	for i := 0; i < ConfigT.Count_tcp_serial; i++ {
		for y := 0; y < int(ConfigT.Tcp_serial[i].Count_node); y++ {
			_type := ConfigT.Tcp_serial[i].Set_node[y].Type_par
			switch _type {
			case 1:
				Count_Anpar = Count_Anpar + int(ConfigT.Tcp_serial[i].Set_node[y].Data_length) // количество аналогов
				addr := ConfigT.Tcp_serial[i].Set_node[y].Address_data                         // адрес в МК такта
				for x := Count_Anpar - int(ConfigT.Tcp_serial[i].Set_node[y].Data_length); x < Count_Anpar; x++ {
					xx := x - (Count_Anpar - int(ConfigT.Tcp_serial[i].Set_node[y].Data_length))
					Buff[x].Mek_104.Ioa = asdu.InfoObjAddr(ConfigT.Tcp_serial[i].Set_node[y].Params[xx].Addres) // делаем адресацию как в модбасе инпутрегистры
					Buff[x].Mod_adress = int(addr) + x                                                          // адрес модбаса в МК
					Buff[x].ID = x                                                                              // номер параметр в массиве
					Buff[x].Mek_104.Time = time.Now()                                                           //
					if s, err := strconv.ParseFloat(ConfigT.Tcp_serial[i].Set_node[y].Params[xx].Hi, 64); err == nil {
						//	fmt.Println(s) // 3.1415927410125732
						Buff[x].Ai_hi = float32(s)
					}
					if s, err := strconv.ParseFloat(ConfigT.Tcp_serial[i].Set_node[y].Params[xx].Low, 64); err == nil {
						//	fmt.Println(s) // 3.1415927410125732
						Buff[x].Ai_low = float32(s)
					}
				}
			case 2:
				//Count_DIpar = Count_DIpar + 16                         // количество дискретов
				indx := ConfigT.Tcp_serial[i].Set_node[y].Index_up     // стартовый индекс
				addr := ConfigT.Tcp_serial[i].Set_node[y].Address_data // адрес в МК такта
				for x := 0; x < 16; x++ {
					Buff_D[x+Count_DIpar].Mek_104.Ioa = asdu.InfoObjAddr(int(indx) + x) // делаем адресацию как в модбасе инпутрегистры
					Buff_D[x+Count_DIpar].Mod_adress = int(addr) + x                    // адрес модбаса в МК
					Buff_D[x+Count_DIpar].ID = int(indx) + x
					Buff_D[x+Count_DIpar].Mek_104.Time = time.Now() // номер параметр в массиве
				}
				Count_DIpar = Count_DIpar + 16
			case 3:
				indx := ConfigT.Tcp_serial[i].Set_node[y].Index_up                     // стартовый индекс
				addr := ConfigT.Tcp_serial[i].Set_node[y].Address_data                 // адрес в МК такта
				Buff_KR[Count_DOpar].Mek_104_On.Ioa = asdu.InfoObjAddr(int(indx))      // Команда Открыть
				Buff_KR[Count_DOpar].Mek_104_Off.Ioa = asdu.InfoObjAddr(int(indx) + 1) // Команда Закрыть
				Buff_KR[Count_DOpar].CRFX.Ioa = asdu.InfoObjAddr(int(indx) - 100)      // Команда сброс ошибок общих
				Buff_KR[Count_DOpar].FDSX.Ioa = asdu.InfoObjAddr(int(indx) + 1000)     // Информация о сработке одновременно двух противоположных концевиков.
				Buff_KR[Count_DOpar].FONX.Ioa = asdu.InfoObjAddr(int(indx) + 1100)     // Информация о том, что после подачи команды на открытие не пришёл сигнал с концевика открыт (IONX) за установленное время (STSX).
				Buff_KR[Count_DOpar].FOFX.Ioa = asdu.InfoObjAddr(int(indx) + 1200)     // Информация о том, что после подачи команды на закрытие не пришёл сигнал с концевика закрыт (IOFX) за установленное время (STSX).
				Buff_KR[Count_DOpar].FFON.Ioa = asdu.InfoObjAddr(int(indx) + 1400)     // Несанкционированный сход с концевика ЗАКРЫТ (несанкционированное открытие/включение)
				Buff_KR[Count_DOpar].FFOF.Ioa = asdu.InfoObjAddr(int(indx) + 1500)     // Несанкционированный сход с концевика ОТКРЫТ (несанкционированное закрытие/выключение)

				Buff_KR[Count_DOpar].Mod_adress = int(addr) // адрес модбаса в МК
				Buff_KR[Count_DOpar].ID = int(ConfigT.Tcp_serial[i].Set_node[y].Address_id)
				Buff_KR[Count_DOpar].Mek_104_Off.Time = time.Now() // номер параметр в массиве
				Count_DOpar = Count_DOpar + 1
			}

		}
	}
	fmt.Printf("Количество Аналогов в настройках : %d\n", Count_Anpar)
	fmt.Printf("Количество Дискретов в настройках : %d\n", Count_DIpar)
	fmt.Printf("Количество Кранов в настройках : %d\n", Count_DOpar)
	return

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

// Перевод из Модбаса в МЭК - на уровне буферов
func read_mod() {

	for {
		for i := 0; i < Count_Anpar; i++ {
			Buff[i].Decod_ACP_Ing(modbus_mk.Buff[i].Val)
		}
		for i := 0; i < Count_DIpar; i++ {
			if Buff_D[i].Mek_104.Value != modbus_mk.Buff_D[i].Val {
				Buff_D[i].Mek_104.Value = modbus_mk.Buff_D[i].Val
				Buff_D[i].Mek_104.Time = time.Now()
				Buff_D[i].Up_Val = true
			}
		}
		for i := 0; i < Count_DIpar; i++ {
			if int(Buff_D[i].Mek_104.Ioa) == 10201 {
				Buff_KR[0].KR_ON = Buff_D[i].Mek_104.Value
				Buff_KR[0].KR_OF = Buff_D[i+1].Mek_104.Value
				Buff_KR[1].KR_ON = Buff_D[i+2].Mek_104.Value
				Buff_KR[1].KR_OF = Buff_D[i+3].Mek_104.Value
				Buff_KR[2].KR_ON = Buff_D[i+4].Mek_104.Value
				Buff_KR[2].KR_OF = Buff_D[i+5].Mek_104.Value
				i = 250
			}
			for i := 0; i < Count_DOpar; i++ {
				// Двойное состояние
				if Buff_KR[i].KR_ON && Buff_KR[i].KR_OF {
					Buff_KR[i].FDSX.Value = true
				} else {
					Buff_KR[i].FDSX.Value = false
				}
				// Включение ИМ
				if Buff_KR[i].KR_ON && Buff_KR[i].COM_ON {
					Buff_KR[i].Done <- true
				}
				// Отключение ИМ
				if Buff_KR[i].KR_OF && Buff_KR[i].COM_OF {
					Buff_KR[i].Done <- true
				}
				//********************** Несанкционированный сход с концевика ЗАКРЫТ (несанкционированное открытие/включение) **************************
				if !Buff_KR[i].KR_OF && Buff_KR[i].IOFX_prvs && !Buff_KR[i].COM_ON {
					Buff_KR[i].FFON.Value = true
				}
				if Buff_KR[i].FFON.Value && (Buff_KR[i].CRFX.Value) {
					Buff_KR[i].FFON.Value = false
				}
				//********************** Несанкционированный сход с концевика ОТКРЫТ (несанкционированное закрытие/выключение) **************************
				if !Buff_KR[i].KR_ON && Buff_KR[i].IONX_prvs && !Buff_KR[i].COM_OF {
					Buff_KR[i].FFOF.Value = true
				}
				if Buff_KR[i].FFOF.Value && (Buff_KR[i].CRFX.Value) {
					Buff_KR[i].FFOF.Value = false
				}
				// Ставить самой последней
				if Buff_KR[i].CRFX.Value {
					Buff_KR[i].CRFX.Value = false
				}
				Buff_KR[i].IONX_prvs = Buff_KR[i].KR_ON
				Buff_KR[i].IOFX_prvs = Buff_KR[i].KR_OF
			}

		}

		//fmt.Println("Кран 1 ", Buff_KR[0].Mek_104.Ioa)
		time.Sleep(time.Millisecond * 100) // было 100
	}

}

// ListenAndServer run the server
func (sf *Server) ListenAndServer(addr string) {

	Ser_Init("config.toml")
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

	//Инициализация сервера МЭК104 переменных
	//asdu.Par_send = Ser_Init("config123.toml")

	modbus_mk.Modbus_up()
	//go modbus_mk.Modbus_up()
	time.Sleep(time.Second * 1)
	go read_mod()
	//	Buff[0].Mek_104.Value = modbus_mk.Buff[0].Val
	// Работа с БД
	//db := InitDB(dbpath)
	//defer db.Close()

	go func() {

		for {

			//time.Sleep(time.Second * 1)
			if Сonnect_srv {
				sf.Debug("UpDate Value in connect")
				time.Sleep(time.Second * 10)
				if len(Par_send) > 0 {
					//	for i := 0; i < len(Par_send); i++ {
					asdu.Transfer_buff(sf, Par_send)
					fmt.Print("Отправил из буфера")
					//	}
					Par_send = nil
				}
			} else {
				sf.Debug("UpDate Value in no connection")
				Check_value()
				time.Sleep(time.Second * 10)
			}

		}
	}()

	for {
		conn, err := listen.Accept()
		if err != nil {
			sf.Error("server run failed, %v", err)
			return
		}

		sf.wg.Add(1)
		go func() {
			Сonnect_srv = true
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
			Сonnect_srv = false
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

// Проверка параметров на изменение и запись новых в буфер со временем
func Check_value() {
	fmt.Printf("cheking values", time.Now(), " time \n")
	for i := 0; i < Count_Anpar; i++ {
		if Buff[i].Up_Val {
			Par_send = append(Par_send, Buff[i])
			fmt.Print("Записал значение ", Buff[i].Mek_104.Value, time.Now(), " time \n")
			Buff[i].Up_Val = false
		}
	}
}
