package modbus_mk

import (
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/goburrow/modbus"
	"github.com/tbrandon/mbserver"
)

const Version = "0.21"

var config TomlConfig                          // структура конфигуратора
var Log *log.Logger                            // это логер для записи
var Debug bool                                 // флаг отладки на экране
var CoilsRed [65536]CoilsRedirect              // массив структур для редиректа записи  Coils на устройства
var HRegRed [65536]HoldingRegisterRedirect     // массив структур для редиректа записи HoldenRegister
var InDiscreteStatus [65536]InputDiscrteStatus //
var InputRegStatus [65536]InputRegistersStatus // статусы обмена Input Registers
var Count_Anpar int                            // количество аналогов
var Count_DIpar int                            // количество дискретов
var Count_DOpar int                            // количество управления
var Buff = make([]Bufer, 100)                  // буфер аналогов
var Buff_D = make([]Bufer_D, 100)              // дискретов дискретов
var Buff_KR = make([]Bufer_KR, 100)            // дискретов дискретов
var Bufer_DO []byte                            // Регистр хранения значений выходных сигналов ДО

/*
Версии:
	0.1 - тестированите Modbus Serial RTU (функция 04), в одиночном serial канале
	0.2 - задание порта обмена (-p) и скорости обмена (-b), флагами в командной строке
	0.3 - введение циклитческого опроса и остановки опроса по Ctrl+C
	0.4 - реализация нескольких комманд опроса Modbus RTU
	0.5 - описываем реализацию slave на serial канале
	0.6 - налало работы компилятора под Windows
	0.7 - Добавил файйл логов
	0.8 - попытка сделать опрос устройства с конфигурацией из toml файа (первого канала в списке и первой ноды)
	0.9 - введение проверок конфигурации из toml файла
	0.10 - добавил сервер для запросов верхнего уровня
	0.11 - оперирую с массивами переменных сервера, coils, input registers, holding registerc
	0.12 - открытие каналов опроса перенес по подпрограмму req
	0.13 - в конфигурации все настройки цифры сделал в uint
	0.14 - сделал правильную обработку дискретов и сохранение на сервере, на сервере массив байт и состояние 0 или 1
	0.15 - добавление записи Coils и HoldenREgisters
	0.16 - работа с симуляторами serial каналов на Linux и Windows (tcp и так одинаковые)
	0.17 - все бьюсь с записью Coils и HoldenREgisters и добавление каналорв по TCP. Команды уже проходят до обработчиков
	0.18 - реализация записи Coils и Holding Registers (одиночное точно работает)
	0.19 - добавляем каналы по TCP
	0.20 - расширенная обработка ошибок при запросах
	0.21 - наполняем обработку ошибок
*/

type chanel struct {
	Enable      bool // true - включено / false - выключено
	Type_chanel int  // тип канала
}

type CoilsRedirect struct {
	ChanelSerial int            // канал для записи Coils с работой по Serial, 0-если не используется
	ChanelTCP    int            // канал для задписи Coils с работой по TCP или UDP
	Address_id   uint8          // адрес устройства на шине
	Address_data uint16         // адрес Coils В устройстве
	icc          chan<- inc_req // канал для передачи данных записи в гороутину
	Time         time.Time      // время последней записи в Coils
	Status       uint8          // статус записи еденичного Ciolsa устройства
}
type KR_registrs_cils struct {
	KR_num       int            // номер крана/канала для управления
	ChanelTCP    int            // канал для задписи Coils с работой по TCP или UDP
	Address_id   uint8          // адрес устройства на шине
	sesion       int            // Открытие ссесии 0-нет 1-открыта
	pred         int            // предварительная команда 0-нет 1-открыть 2-закрыть
	TU           int            // команда 0-нет 1-открыть 2-закрыть
	Address_data uint16         // адрес Coils В устройстве начальный
	icc          chan<- inc_req // канал для передачи данных записи в гороутину
	Time         time.Time      // время последней записи в Coils
	Status       uint8          // статус записи еденичного Ciolsa устройства
}

type HoldingRegisterRedirect struct {
	ChanelSerial int            // канал для записи Coils с работой по Serial, 0-если не используется
	ChanelTCP    int            // канал для задписи Coils с работой по TCP или UDP
	Address_id   uint8          // адрес устройства на шине
	Address_data uint16         // адрес Coils В устройстве
	icc          chan<- inc_req // какал для передачи данных записи в гороутину
	Time         time.Time      // время последней записи в Coils
	Status       uint8          // статус записи еденичного Ciolsa устройства
}

// структура для описания работы каналов ввода input distrete
type InputDiscrteStatus struct {
	EnableInputDiscrete bool      // true - у нас используется адрес для запросов input discrete
	Time                time.Time // время последнего обращения
	Status              uint8     // статус устройства
}

// структура для описания работы каналов ввода input registers
type InputRegistersStatus struct {
	EnableInputRegisters bool      // true - у нас используется адрес для запросов input discrete
	Time                 time.Time // время последнего обращения
	Status               uint8     // статус устройства
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

// Структура буфера для аналоговых параметров для передачи в МЭК
type Bufer struct {
	ID  int
	Val float32
}

// Структура буфера для дискретных параметров для передачи в МЭК
type Bufer_D struct {
	ID  int
	Val bool
}

// Структура буфера для управления каранами для передачи в МЭК
type Bufer_KR struct {
	KR_sel      bool
	Send_cancel bool
	CMD         bool
	Num_chanel  int
}

// ***************************************************************************************
// Проверка на ошибку для уменьшения писанины
func err_log(err3 error, result []byte) {
	if err3 != nil {
		Log.Printf("**ERROR** Chanel: %s", result)
		os.Exit(-1)
	}
}

// структура для передачи запроса на исполнение команды с данными в канал с необходимым нам устройством Modbus RTU
type inc_req struct {
	Address_id   uint8       // адрес устройства на Modbus RTU
	Command      uint8       // команда для исполнения по Modbus RTU (5,6,15,16 - WriteSingle(Multiple)Coils/HoldingRegisters)
	Address_data uint16      // адрес данных в устройстве
	Data_length  uint16      // длинна запрашиваемых-передаваемых данных
	Data         [125]uint16 // данные для операции (максимальня длинна)
	Pos          uint16      // позиция первого параметра в карте памяти Coils или HildingRegisters для записи Time и Status последней операции
}

// ***************************************************************************************
// запрос необходимых данных от устройства
var kr KR_registrs_cils

// ***************************************************************************************
// запрос необходимых данных от устройства
// chanel - указатель на структуру канала опроса устройств
// sever - сервер modbus для записи данныых опроса устройств
func req_serial(server *mbserver.Server, chanel *Set_serial, cc <-chan struct{}, inc <-chan inc_req) {
	var xx inc_req
	handler := modbus.NewRTUClientHandler(chanel.Port_tty) // с версии 0.17 пишем порты напямую, можно COM-порты пускать под Wndows

	//	handler.RS485.Enabled = true
	handler.RS485.Enabled = false // не может быть true - драйвер для allwinner H3 не может это сделать, а ch344 делает это аппаратно
	handler.BaudRate = int(chanel.Baud)
	//	handler.DataBits = 8
	handler.DataBits = int(chanel.Bits)
	//	handler.Parity = "N"
	handler.Parity = chanel.Parity
	//	handler.StopBits = 1
	handler.StopBits = int(chanel.Stop)
	handler.SlaveId = 1
	handler.Timeout = time.Millisecond * time.Duration(200)
	err := handler.Connect()
	defer handler.Close()
	if err != nil {
		// пока нет расширеннной обработки ошибки запроса - просто выход
		if Debug {
			fmt.Printf(err.Error())
		}
		Log.Printf("**ERROR** REQ: No open Serial Port chanel: '%s'\r\n", chanel.Port_tty)
		Log.Printf(err.Error())
		os.Exit(-1) // критическая ошибка !!!!!
		//return // просто выходим из подпрограмы опроса канала
	}

	client := modbus.NewClient(handler)

	for {
		// пока обработка только одной ноды оз конфигурации
		for count := 0; count < int(chanel.Count_node); count++ {
			if handler.SlaveId != chanel.Set_node[count].Address_id {
				// надо опробывать без закрытия handler
				//				handler.Close()
				handler.SlaveId = chanel.Set_node[count].Address_id
				//				handler.Connect()
				//				defer handler.Close()
				client = modbus.NewClient(handler) // перезапустим клиента
			}
			switch chanel.Set_node[count].Command {
			// *************************************
			case 04:
				result4, err2 := client.ReadInputRegisters(uint16(chanel.Set_node[count].Address_data), uint16(chanel.Set_node[count].Data_length))
				if err2 != nil {
					// пока нет расширеннной обработки ошибки запроса - просто выход - пора расширять
					Log.Printf("**ERROR** Read Input Register Chanel: %s\n", chanel.Port_tty)
					Log.Printf(err2.Error())
					if Debug {
						fmt.Printf(err2.Error())
					}
					InputRegStatus[chanel.Set_node[count].Index_up].Status = 0x10     // ошибка, чегото там, пока не декодируем
					InputRegStatus[chanel.Set_node[count].Index_up].Time = time.Now() // время последней ошибки

					//					os.Exit(-1) // пока выходим, не делаем обработку
				} else {
					if Debug {
						fmt.Printf("Chanel: %s,\tRead Input Regires(4):\t\tnode %d: %v\n", chanel.Port_tty, count, result4)
					}
					// можно сохранить в памяти сервера
					//					_, err := client_local.WriteMultipleRegisters(uint16(chanel.Set_node[count].Index_up), uint16(chanel.Set_node[count].Data_length), result4)
					new_data := mbserver.BytesToUint16(result4)
					for ii := 0; ii < int(chanel.Set_node[count].Data_length); ii++ {
						server.InputRegisters[ii+int(chanel.Set_node[count].Index_up)] = new_data[ii]
					}
				}
				// *************************************
			case 01:
				result1, err2 := client.ReadCoils(uint16(chanel.Set_node[count].Address_data), uint16(chanel.Set_node[count].Data_length))
				if err2 != nil {
					// пока нет расширеннной обработки ошибки запроса - просто выход !!!!!
					Log.Printf("**ERROR** Read Coils Chanel: %s", chanel.Port_tty)
					Log.Printf(err2.Error())
					fmt.Printf(err2.Error())
					os.Exit(-1)
				} else {
					if Debug {
						fmt.Printf("Chanel: %s,\tRead Coils(1):\t\t\tnode %d: %v\n", chanel.Port_tty, count, result1)
					}
					// можно сохранить в памяти сервера
					//					_, err := client_local.WriteMultipleCoils(uint16(chanel.Set_node[count].Index_up), uint16(chanel.Set_node[count].Data_length), result2)
					new_data := result1
					for i := 0; i < len(new_data); i++ {
						for b := 0; b < 8; b++ {
							if (b + i*8) < int(chanel.Set_node[count].Data_length) {
								if (new_data[i] & byte(1<<b)) != 0 {
									server.Coils[i*8+b+int(chanel.Set_node[count].Index_up)] = 1
								} else {
									server.Coils[i*8+b+int(chanel.Set_node[count].Index_up)] = 0
								}
							} else {
								goto OutLoop2
							}
						}
					}
				OutLoop2:
				}
				// *************************************
			case 02:
				result2, err2 := client.ReadDiscreteInputs(uint16(chanel.Set_node[count].Address_data), uint16(chanel.Set_node[count].Data_length))
				if err2 != nil {
					// пока нет расширеннной обработки ошибки запроса - просто выход !!!!!
					Log.Printf("**ERROR** Read Input Discrete Chanel: %s", chanel.Port_tty)
					Log.Printf(err2.Error())
					fmt.Printf(err2.Error())
					os.Exit(-1)
				} else {
					if Debug {
						fmt.Printf("Chanel: %s,\tRead Input Discretes(2):\tnode %d: %v\n", chanel.Port_tty, count, result2)
					}
					// можно сохранить в памяти сервера
					//	_, err := client_local.WriteMultipleCoils(uint16(chanel.Set_node[count].Index_up), uint16(chanel.Set_node[count].Data_length), result2)
					new_data := result2
					/*	for ii := 0; ii < int(chanel.Set_node[count].Data_length); ii++ {
						//	server.DiscreteInputs[ii+int(chanel.Set_node[count].Index_up)] = new_data[ii]
							server.DiscreteInputs[ii+int(chanel.Set_node[count].Index_up)] = result2[ii]
						}
					*/
					for i := 0; i < len(new_data); i++ {
						for b := 0; b < 8; b++ {
							if (b + i*8) < int(chanel.Set_node[count].Data_length) {
								if (new_data[i] & byte(1<<b)) != 0 {
									server.DiscreteInputs[i*8+b+int(chanel.Set_node[count].Index_up)] = 1
								} else {
									server.DiscreteInputs[i*8+b+int(chanel.Set_node[count].Index_up)] = 0
								}
							} else {
								goto OutLoop1
							}
						}
					}
				OutLoop1:
				}
				// *************************************

			case 03:
				result3, err2 := client.ReadHoldingRegisters(uint16(chanel.Set_node[count].Address_data), uint16(chanel.Set_node[count].Data_length))
				if err2 != nil {
					// пока нет расширеннной обработки ошибки запроса - просто выход !!!!!
					Log.Printf("**ERROR** Read Holding Register Chanel: %s", chanel.Port_tty)
					Log.Printf(err2.Error())
					fmt.Printf(err2.Error())
					os.Exit(-1)
				} else {
					if Debug {
						//						fmt.Printf(": node%d: %v\n", count, resulTimet3)
						fmt.Printf("Chanel: %s,\tRead Holding Registers(3):\tnode %d: %v\n", chanel.Port_tty, count, result3)
					}
					// можно сохранить в памяти сервера
					//	_, err := client_local.WriteMultipleRegisters(uint16(chanel.Set_node[count].Index_up), uint16(chanel.Set_node[count].Data_length), result3)
					new_data := mbserver.BytesToUint16(result3)
					for ii := 0; ii < int(chanel.Set_node[count].Data_length); ii++ {
						server.HoldingRegisters[ii+int(chanel.Set_node[count].Index_up)] = new_data[ii]
					}
					// if err != nil {
					//	fmt.Printf(err.Error())
					//	}
				}
			default:
				// пока ошибки toml на уровне описания ноды не обрабатываю
				Log.Printf("**ERROR**, Chanel %s, error to toml file, code Command in node %d\r\n", chanel.Port_tty, count)
				os.Exit(-1) // ошибка критическия надо править toml !!!!!
			}
			// time.Sleep(time.Millisecond * time.Duration(config.Tty_serial[0].Time_loop)) // время цикла запроса передать из config
			select {
			case xx = <-inc: // принмаем данные на обработку
				if Debug {
					fmt.Printf("\t----- Execute COMMAND: '%d' for chanel '%s'\r\n", xx.Command, chanel.Port_tty)
				}
				// можно  разбирать, что выдавать и куда
				switch xx.Command {
				case 05: // Write Single Coils
					if handler.SlaveId != xx.Address_id {
						//						handler.Close()
						handler.SlaveId = xx.Address_id
						//						handler.Connect()
						//						defer handler.Close()
						client = modbus.NewClient(handler) // перезапустим клиента
					}
					write5, err := client.WriteSingleCoil(xx.Address_data, xx.Data[0])
					CoilsRed[xx.Pos].Time = time.Now() // последнее время записи в Coils
					if err != nil {
						// ошибка записи Single Copils
						Log.Printf("**ERROR** Write Single Coils, Chanel: %s AddrID: %d\r\n", chanel.Port_tty, xx.Address_id)
						Log.Printf("**data Responce: %v", write5)
						CoilsRed[xx.Pos].Status = 2 // ошибка записи в !!!!!
					}
				case 06: // Write Single Holding Regster
					if handler.SlaveId != xx.Address_id {
						//						handler.Close()
						handler.SlaveId = xx.Address_id
						//						handler.Connect()
						//						defer handler.Close()
						client = modbus.NewClient(handler) // перезапустим клиента
					}
					write6, err := client.WriteSingleRegister(xx.Address_data, xx.Data[0])
					HRegRed[xx.Pos].Time = time.Now() // последнее время записи в Coils
					if err != nil {
						// ошибка записи Single Holding REgister
						Log.Printf("**ERROR** Write Single Holding Register, Chanel: %s AddrID: %d\r\n", chanel.Port_tty, xx.Address_id)
						Log.Printf("**data Responce: %v", write6)
						HRegRed[xx.Pos].Status = 2 // ошибка записи - надо ввести   типизацию кодов !!!!!
					} else {

					}
				case 15: // Write  Multiple Coils
				case 16: // Write Multiple Holding Regster
				default: // непонятная команда пришла в канал
				}
			default:
			}
		} //for
		// неблокирующий прием данных из канала
		select {
		case <-cc: // получили сигнал на завершение
			break
			//default: // обязательно для получения неблокирующий функции
			//time.Sleep(time.Millisecond * time.Duration(config.Tty_serial[0].Time_loop)) // время цикла запроса передать из config
		}
	}
}

//func (s *Server) RegisterFunctionHandler(funcCode uint8, function func(*Server, Framer) ([]byte, *Exception)

// ***************************************************************************************
// функция обработки записи еденичного Coil (5)
func WrSingleCoilsOverr(server *mbserver.Server) {
	var xx KR_registrs_cils
	server.RegisterFunctionHandler(5,
		func(s *mbserver.Server, frame mbserver.Framer) ([]byte, *mbserver.Exception) {
			data1 := frame.GetData()
			register := int(binary.BigEndian.Uint16(data1[0:2])) // адрес регистра
			//numRegs := int(binary.BigEndian.Uint16(data1[2:4]))  // это как раз данные для записи 0xFF00 - on, 0x0000 - off
			xx.Address_id = CoilsRed[register].Address_id     // адрес устройства на шине modbus
			xx.Address_data = CoilsRed[register].Address_data // адрес данных в устройстве
			xx.Time = time.Now()
			//if numRegs > 0 {
			//	s.Coils[register] = 1
			//	switch register {
			//	// *************************************
			//	case int(config.Tcp_serial[0].Set_node[5].Index_up):
			//		fmt.Printf("\t>>>>> Сеанс управление закрыт: %v\r\n", uint8(register))
			//		kr.sesion = 1
			//	case int(config.Tcp_serial[0].Set_node[5].Index_up - 6):
			//		fmt.Printf("\t>>>>> Предварительная закрыть: %v\r\n", uint8(register))
			//		kr.pred = 2
			//	case int(config.Tcp_serial[0].Set_node[5].Index_up - 5):
			//		fmt.Printf("\t>>>>> Управление краном закрыть: %v\r\n", uint8(register))
			//		kr.TU = 2
			//	}
			//
			//} else {
			//	s.Coils[register] = 0
			//	switch register {
			//	// *************************************
			//	case int(config.Tcp_serial[0].Set_node[5].Index_up):
			//		fmt.Printf("\t>>>>> Сеанс управление открыт: %v\r\n", uint8(register))
			//		kr.sesion = 0
			//	case int(config.Tcp_serial[0].Set_node[5].Index_up - 6):
			//		fmt.Printf("\t>>>>> Предварительная открыть: %v\r\n", uint8(register))
			//		kr.pred = 1
			//	case int(config.Tcp_serial[0].Set_node[5].Index_up - 5):
			//		fmt.Printf("\t>>>>> Управление краном открыть: %v\r\n", uint8(register))
			//		kr.TU = 1
			//	}
			//}
			return data1, &mbserver.Success
		})
}

// ***************************************************************************************
// функция обработки записи цепочки Coilы (15)
func WrMultipleCoilsOverr(server *mbserver.Server) {
	server.RegisterFunctionHandler(15,
		func(s *mbserver.Server, frame mbserver.Framer) ([]byte, *mbserver.Exception) {
			data1 := frame.GetData()
			register := int(binary.BigEndian.Uint16(data1[0:2]))
			numRegs := int(binary.BigEndian.Uint16(data1[2:4]))
			//			endRegister := register + numRegs
			//		register, numRegs, endRegister := frame.registerAddressAndNumber()
			data := make([]byte, 4)
			data[0] = CoilsRed[register].Address_id
			data[1] = 5 // запись еденичного Coils
			data[2] = byte(CoilsRed[register].Address_data)
			if Debug {
				fmt.Printf("\t>>>>> Write Multiple Coils: %v\r\n", data1)
				//				fmt.Printf("register: %d, numRegs: 0x%2F, endRegister %d\r\n", register, numRegs, endRegister)
				h := fmt.Sprintf("%04x", numRegs)
				fmt.Printf("\t>>>>> register: %d, data: 0x%s\r\n", register, h)
			}
			/*
				dataSize := numRegs / 8
				data := make([]byte, 1+dataSize)
				data[0] = byte(dataSize)
				for i := range s.DiscreteInputs[register:endRegister] {
					// Return all 1s, regardless of the value in the DiscreteInputs array.
					shift := uint(i) % 8
					data[1+i/8] |= byte(1 << shift)
				}
			*/
			return data1, &mbserver.Success
		})
}

// ***************************************************************************************
// функция обработки записи еденичного Register (6)
func WrSingleRegisterOverr(server *mbserver.Server) {
	var xx inc_req
	server.RegisterFunctionHandler(6,
		func(s *mbserver.Server, frame mbserver.Framer) ([]byte, *mbserver.Exception) {
			data1 := frame.GetData()
			register := int(binary.BigEndian.Uint16(data1[0:2]))
			numRegs := int(binary.BigEndian.Uint16(data1[2:4]))
			endRegister := register + numRegs
			//		register, numRegs, endRegister := frame.registerAddressAndNumber()
			xx.Address_id = HRegRed[register].Address_id     // адрес устройства на щине modbus
			xx.Command = 6                                   // запись еденичного Holding Register
			xx.Address_data = HRegRed[register].Address_data // адрес данных в устройстве
			xx.Pos = uint16(register)                        // позиция в табюлице реверса для записи статусов
			xx.Data[0] = uint16(numRegs)                     // данные по записи
			xx.Data_length = 1                               // у нас один регистр
			HRegRed[register].icc <- xx                      // перезадим данные в нужную нам горутину

			if Debug {
				fmt.Printf("\t>>>>> Write Single Registers: %v\r\n", data1)
				fmt.Printf("\t>>>>> register: %d, numRegs: %d, endRegister %d\r\n", register, numRegs, endRegister)
			}
			//			нeужен свой расклад, что, куда писать из массива CoilsRed

			//			dataSize := numRegs / 8
			//			data := make([]byte, 1+dataSize)
			//			data[0] = byte(dataSize)
			//			for i := range s.DiscreteInputs[register:endRegister] {
			//				// Return all 1s, regardless of the value in the DiscreteInputs array.
			//				shift := uint(i) % 8
			//				data[1+i/8] |= byte(1 << shift)
			//			}
			return data1, &mbserver.Success
		})
}

// ***************************************************************************************
// функция обработки записи цепочки Registers (16)
func WrMultipleRegisterOverr(server *mbserver.Server) {
	server.RegisterFunctionHandler(16,
		func(s *mbserver.Server, frame mbserver.Framer) ([]byte, *mbserver.Exception) {
			data1 := frame.GetData()
			register := int(binary.BigEndian.Uint16(data1[0:2]))
			numRegs := int(binary.BigEndian.Uint16(data1[2:4]))
			endRegister := register + numRegs
			//		register, numRegs, endRegister := frame.registerAddressAndNumber()
			if endRegister > 65535 { // слишком большай адрес
				return []byte{}, &mbserver.IllegalDataAddress
			}
			data := make([]byte, 4)
			data[0] = CoilsRed[register].Address_id
			data[1] = 5 // запись еденичного Coils
			data[2] = byte(CoilsRed[register].Address_data)
			if Debug {
				fmt.Printf("\t>>>>> Write Multiple Hilding Registers: %v\r\n", data1)
				fmt.Printf("\t>>>>> register: %d, numRegs: %d, endRegister %d\r\n", register, numRegs, endRegister)
				//				h := fmt.Sprintf("%04x", numRegs)
				//				fmt.Printf("\t>>>>>register: %d, data: 0x%s\r\n", register, h)
			}
			/*
				dataSize := numRegs / 8
				data := make([]byte, 1+dataSize)
				data[0] = byte(dataSize)
				for i := range s.DiscreteInputs[register:endRegister] {
					// Return all 1s, regardless of the value in the DiscreteInputs array.
					shift := uint(i) % 8
					data[1+i/8] |= byte(1 << shift)
				}
			*/
			return data1, &mbserver.Success
		})
}

//****************************************************************************************
// функция управления кранами

// ***************************************************************************************
// з
func req_tcp_serial(chanel *Set_tcp, cc <-chan struct{}, inc <-chan inc_req, arr []Bufer) {
	var xx inc_req
	//var timer1 time.Timer
	//	handler := modbus.NewRTUClientHandler(chanel.Port_tty) // с версии 0.17 пишем порты напямую, можно COM-порты пускать под Wndows
	handler := modbus.NewTCPClientHandler(chanel.Ip + ":" + strconv.Itoa(chanel.Port)) // с версии 0.17 пишем порты напямую, можно COM-порты пускать под Wndows
	handler.IdleTimeout = 10                                                           //  тайл-аут на операции
	handler.SlaveId = 1

	err := handler.Connect()
	defer handler.Close()

	if err != nil {
		// пока нет расширеннной обработки ошибки запроса - просто выход
		if Debug {
			fmt.Printf(err.Error())
		}
		Log.Printf("**ERROR** REQ: No open Serial Port chanel: '%s'\r\n", chanel.Ip)
		Log.Printf(err.Error())
		os.Exit(-1) // критическая ошибка !!!!!
		//return // просто выходим из подпрограмы опроса канала
	} else {

		client := modbus.NewClient(handler)
		//	Buff := make([]Bufer, Count_Anpar)     // для передачи наверх виден из модуля cp104
		//	Buff_D := make([]Bufer_D, Count_DIpar) // для передачи наверх виден из модуля cp104
		//	Buff_KR := make([]Bufer_KR, Count_DOpar) // для передачи наверх виден из модуля cp104
		//var array = make([]Bufer, Count_Anpar)     // временный буфер для аналогов
		var array []Bufer     // временный буфер для аналогов
		var array_d []Bufer_D // временный буфер для дискретов
		//var array_kr []Bufer_KR // временный буфер для дискретов

		for {

			// пока обработка только одной ноды оз конфигурации
			for count := 0; count < int(chanel.Count_node); count++ {
				if handler.SlaveId != chanel.Set_node[count].Address_id {
					handler.SlaveId = chanel.Set_node[count].Address_id
					client = modbus.NewClient(handler) // перезапустим клиента
				}
				switch chanel.Set_node[count].Command {
				// *************************************
				case 04:
					result4, err2 := client.ReadInputRegisters(uint16(chanel.Set_node[count].Address_data), uint16(chanel.Set_node[count].Data_length))
					if err2 != nil {
						// пока нет расширеннной обработки ошибки запроса - просто выход - пора расширять
						Log.Printf("**ERROR** TCP Read Input Register Chanel: %s", chanel.Ip)
						Log.Printf(err2.Error())
						fmt.Printf(err2.Error())
						os.Exit(-1)
					} else {
						if Debug {
							fmt.Printf("Chanel: %s,\tRead Input Regires(4):\t\tnode %d: %v\n", chanel.Ip, count, result4)
						}
						// можно сохранить в памяти сервера
						//					_, err := client_local.WriteMultipleRegisters(uint16(chanel.Set_node[count].Index_up), uint16(chanel.Set_node[count].Data_length), result4)
						//	new_data := mbserver.BytesToUint16(result4)
						for ii := 0; ii < int(chanel.Set_node[count].Data_length); ii++ {
							//		server.InputRegisters[ii+int(chanel.Set_node[count].Index_up)] = new_data[ii]
						}
					}
					// *************************************
				case 01: // Для такт у не нужна
					result1, err2 := client.ReadCoils(uint16(chanel.Set_node[count].Address_data), uint16(chanel.Set_node[count].Data_length))
					if err2 != nil {
						// пока нет расширеннной обработки ошибки запроса - просто выход !!!!!
						Log.Printf("**ERROR** Read Coils Chanel: %s", chanel.Ip)
						Log.Printf(err2.Error())
						fmt.Printf(err2.Error())
						os.Exit(-1)
					} else {
						if Debug {
							fmt.Printf("Chanel: %s,\tRead Coils(1):\t\t\tnode %d: %v\n", chanel.Ip, count, result1)
						}
						// можно сохранить в памяти сервера
						//					_, err := client_local.WriteMultipleCoils(uint16(chanel.Set_node[count].Index_up), uint16(chanel.Set_node[count].Data_length), result2)
						new_data := result1
						for i := 0; i < len(new_data); i++ {
							for b := 0; b < 8; b++ {
								if (b + i*8) < int(chanel.Set_node[count].Data_length) {
									if (new_data[i] & byte(1<<b)) != 0 {
										//						server.Coils[i*8+b+int(chanel.Set_node[count].Index_up)] = 1
									} else {
										//						server.Coils[i*8+b+int(chanel.Set_node[count].Index_up)] = 0
									}
								} else {
									goto OutLoop2
								}
							}
						}
					OutLoop2:
					}
					// *************************************
				case 02:
					result2, err2 := client.ReadDiscreteInputs(uint16(chanel.Set_node[count].Address_data), uint16(chanel.Set_node[count].Data_length))
					if err2 != nil {
						// пока нет расширеннной обработки ошибки запроса - просто выход !!!!!
						Log.Printf("**ERROR** Read Input Discretes Chanel: %s", chanel.Ip)
						Log.Printf(err2.Error())
						fmt.Printf(err2.Error())
						os.Exit(-1)
					} else {
						if Debug {
							fmt.Printf("Chanel: %s,\tRead Input Discretes(2):\tnode %d: %v\n", chanel.Ip, count, result2)
						}
						// можно сохранить в памяти сервера
						//	_, err := client_local.WriteMultipleCoils(uint16(chanel.Set_node[count].Index_up), uint16(chanel.Set_node[count].Data_length), result2)
						new_data := result2
						for i := 0; i < len(new_data); i++ {
							for b := 0; b < 8; b++ {
								if (b + i*8) < int(chanel.Set_node[count].Data_length) {
									if (new_data[i] & byte(1<<b)) != 0 {
										//						server.DiscreteInputs[i*8+b+int(chanel.Set_node[count].Index_up)] = 1
									} else {
										//						server.DiscreteInputs[i*8+b+int(chanel.Set_node[count].Index_up)] = 0
									}
								} else {
									goto OutLoop1
								}
							}
						}
					OutLoop1:
					}
					// *************************************
				case 03:

					result3, err2 := client.ReadHoldingRegisters(uint16(chanel.Set_node[count].Address_data), uint16(chanel.Set_node[count].Data_length))
					if err2 != nil {
						// пока нет расширеннной обработки ошибки запроса - просто выход !!!!!
						Log.Printf("**ERROR** Read Holding Register - Chanel: %s", chanel.Ip)
						Log.Printf(err2.Error())
						fmt.Printf(err2.Error())
						os.Exit(-1)
					} else {
						if Debug {
							fmt.Printf("Chanel: %s,\tRead Holding Registers(3):\tnode %d: %v\n", chanel.Ip, count, result3)
						}
						// можно сохранить в памяти сервера
						//	_, err := client_local.WriteMultipleRegisters(uint16(chanel.Set_node[count].Index_up), uint16(chanel.Set_node[count].Data_length), result3)
						new_data := mbserver.BytesToUint16(result3)
						for ii := 0; ii < int(chanel.Set_node[count].Data_length); ii++ {
							if chanel.Set_node[count].Type_par == 1 { // Пишем регистр в область InputRegisters
								var val = Bufer{ii + int(chanel.Set_node[count].Index_up), float32(new_data[ii])}
								array = append(array, val)

							}
							if chanel.Set_node[count].Type_par == 2 { // Разбираем Слово на биты если нам надо прочитать дискретный вход
								buf := new_data[ii]
								for j := 0; j < 16; j++ {
									if (buf>>j)&1 == 1 {
										var val_d = Bufer_D{j + int(chanel.Set_node[count].Index_up), true}
										array_d = append(array_d, val_d)
									} else {
										var val_d = Bufer_D{j + int(chanel.Set_node[count].Index_up), false}
										array_d = append(array_d, val_d)
									}

								}
								//st := strconv.FormatUint(uint64(buf), 10)
								//fmt.Printf("%s", st)
							}
						}

					}
				case 05: // Write Single Coils
					if handler.SlaveId != 1 {
						handler.SlaveId = 1
						client = modbus.NewClient(handler) // перезапустим клиента
					}
					//	timer1 := time.NewTimer(2 * time.Second)
					for ii := 0; ii < Count_DOpar; ii++ {
						// Если пришла команда закрытия и есть сеанс
						//var Seans_KR_OFF bool //= 0 // server.Coils[int(chanel.Set_node[count].Index_up)+ii] == 0 && kr.TU == 1 && !(server.DiscreteInputs[int(chanel.Set_node[count-1].Index_up)+4] == 1) // Если пришла команда закрытия и есть сеанс
						//var Seans_KR_ON bool  //=1 // server.Coils[int(chanel.Set_node[count].Index_up)+ii] == 0 && kr.TU == 2 && !(server.DiscreteInputs[int(chanel.Set_node[count-1].Index_up)+5] == 1)  // Если пришла команда открытия и есть сеанс
						result4, err3 := client.ReadHoldingRegisters(uint16(30), uint16(1)) // Вычитываем что в регистре управления DO
						err_log(err3, result4)

						if Buff_KR[ii].KR_sel && Buff_KR[ii].CMD { // исполняем команду

							result5, err3 := client.WriteSingleRegister(uint16(chanel.Set_node[count].Address_data), binary.BigEndian.Uint16(result4)|uint16(1<<Buff_KR[ii].Num_chanel))
							err_log(err3, result5)
							Buff_KR[ii].KR_sel = false
							Buff_KR[ii].CMD = false
							//timer1 := time.AfterFunc(time.Second*10, func() {
							//	result4, err3 := client.ReadHoldingRegisters(uint16(30), uint16(1)) // Вычитываем что в регистре управления DO
							//	err_log(err3, result4)
							//	result5, err3 := client.WriteSingleRegister(uint16(chanel.Set_node[count].Address_data), binary.BigEndian.Uint16(result4)^uint16(1<<Buff_KR[ii].Num_chanel))
							//	err_log(err3, result5)
							//	Buff_KR[ii].Send_cancel = false
							//})
							//<-timer1.C
						}
						if Buff_KR[ii].Send_cancel { // сбрасываем команду
							result5, err3 := client.WriteSingleRegister(uint16(chanel.Set_node[count].Address_data), binary.BigEndian.Uint16(result4)^uint16(1<<Buff_KR[ii].Num_chanel))
							err_log(err3, result5)
							Buff_KR[ii].Send_cancel = false
						}
						//else { // Если пришла команда открытия и есть сеанс
						//	if Seans_KR_ON {
						//		result4, err3 := client.ReadHoldingRegisters(uint16(36), uint16(1)) // Вычитываем что в регистре управления DO
						//		err_log(err3, result4)
						//		if kr.pred != 2 { // Если отмена
						//			result5, err3 := client.WriteSingleRegister(uint16(chanel.Set_node[count].Address_data), uint16(binary.LittleEndian.Uint16(result4)|uint16(00000001<<2)))
						//			err_log(err3, result5)
						//			//if timer1.Stop() {
						//			//	timer1 := time.NewTimer(10 * time.Second)
						//			//	<-timer1.C
						//			//	fmt.Printf("\t>>>>> Отработал таймер 10 сек: %v\r\n", timer1)
						//			//}
						//
						//		} else {
						//			result5, err3 := client.WriteSingleRegister(uint16(chanel.Set_node[count].Address_data), uint16(binary.LittleEndian.Uint16(result4)^uint16(00000001<<2)))
						//			err_log(err3, result5)
						//		}
						//	}
						//	// Если сеанс закрыли сбрасываем в 00
						//	if !Seans_KR_ON && !Seans_KR_OFF {
						//		result4, err3 := client.ReadHoldingRegisters(uint16(36), uint16(1)) // Вычитываем что в регистре управления DO
						//		err_log(err3, result4)
						//		client.WriteSingleRegister(uint16(chanel.Set_node[count].Address_data), uint16(binary.LittleEndian.Uint16(result4)&uint16(00)))
						//		kr.TU = 0 //Сброс ТУ
						//	}
						//}
						if err != nil {
							// ошибка записи Single Copils
							Log.Printf("**ERROR** Write Single Coils, Chanel: %s AddrID: %d\r\n", chanel.Ip, 1)
							//Log.Printf("**data Responce: %v", write5)
							CoilsRed[xx.Pos].Status = 2 // ошибка записи в !!!!!
						}
					}
				default:
					// пока ошибки toml на уровне описания ноды не обрабатываю
					Log.Printf("**ERROR**, Chanel %s, error to toml file, code Command in node %d\r\n", chanel.Ip, count)
					os.Exit(-1) // ошибка критическия надо править toml !!!!!
				}
				// time.Sleep(time.Millisecond * time.Duration(config.Tty_serial[0].Time_loop)) // время цикла запроса передать из config
				select {
				case xx = <-inc: // принмаем данные на обработку
					if Debug {
						fmt.Printf("\t----- Execute COMMAND: '%d' for chanel '%s'\r\n", xx.Command, chanel.Ip)
					}
					// можно  разбирать, что выдавать и куда
					switch xx.Command {
					case 05: // Write Single Coils
						if handler.SlaveId != xx.Address_id {
							//						handler.Close()
							handler.SlaveId = xx.Address_id
							//						handler.Connect()
							//						defer handler.Close()
							client = modbus.NewClient(handler) // перезапустим клиента
						}
						write5, err := client.WriteSingleCoil(xx.Address_data, xx.Data[0])
						CoilsRed[xx.Pos].Time = time.Now() // последнее время записи в Coils
						if err != nil {
							// ошибка записи Single Copils
							Log.Printf("**ERROR** Write Single Coils, Chanel: %s AddrID: %d\r\n", chanel.Ip, xx.Address_id)
							Log.Printf("**data Responce: %v", write5)
							CoilsRed[xx.Pos].Status = 2 // ошибка записи в !!!!!
						}
					case 06: // Write Single Holding Regster
						if handler.SlaveId != xx.Address_id {
							//						handler.Close()
							handler.SlaveId = xx.Address_id
							//						handler.Connect()
							//						defer handler.Close()
							client = modbus.NewClient(handler) // перезапустим клиента
						}
						write6, err := client.WriteSingleRegister(xx.Address_data, xx.Data[0])
						HRegRed[xx.Pos].Time = time.Now() // последнее время записи в Coils
						if err != nil {
							// ошибка записи Single Holding REgister
							Log.Printf("**ERROR** Write Single Holding Register, Chanel: %s AddrID: %d\r\n", chanel.Ip, xx.Address_id)
							Log.Printf("**data Responce: %v", write6)
							HRegRed[xx.Pos].Status = 2 // ошибка записи - надо ввести   типизацию кодов !!!!!
						} else {

						}
					case 15: // Write  Multiple Coils
					case 16: // Write Multiple Holding Regster
					default: // непонятная команда пришла в канал
					}
				default:
				}
			} //for
			// неблокирующий прием данных из канала
			select {
			case <-cc: // получили сигнал на завершение
				break
			default: // обязательно для получения неблокирующий функции
				time.Sleep(time.Millisecond * time.Duration(config.Tcp_serial[0].Time_loop)) // время цикла запроса передать из config
			}
			copy(Buff, array)
			array = nil
			copy(Buff_D, array_d)
			array_d = nil
			//		array_kr = nil

		}
	}
}

// ***************************************************************************************
// Головная функция, однако
func Modbus_up() {

	flag.BoolVar(&Debug, "d", false, "Debug print read parametrs")
	flag.Parse()
	_, errt := toml.DecodeFile("config.toml", &config)
	if errt != nil {
		Log.Println("Eror load *.toml file")
		Log.Println(errt)
		os.Exit(-1)
	}
	for i := 0; i < config.Count_tcp_serial; i++ {
		for y := 0; y < int(config.Tcp_serial[i].Count_node); y++ {

			_type := config.Tcp_serial[i].Set_node[y].Type_par
			switch _type {
			case 1:
				Count_Anpar = Count_Anpar + int(config.Tcp_serial[i].Set_node[y].Data_length)
			case 2:
				Count_DIpar = Count_DIpar + 16
			case 3:
				Count_DOpar = Count_DOpar + int(config.Tcp_serial[i].Set_node[y].Data_length)
			}
		}
	}
	// пытаемся открыть файл для записи лога
	fl, errl := os.OpenFile("test_dm04.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	if errl != nil {
		panic(errl.Error()) // proper error handling instead of panic in your app
	}
	defer fl.Close() // закрываем после выхода
	Log = log.New(fl, "", log.Ldate|log.Ltime|log.LUTC)

	fmt.Printf("START PROGRAM TEST DM04: v%s !!!!\r\n", Version)
	Log.Printf("\n\r\n\r****-----------------------------------------------------------****\n")
	Log.Printf("START PROGRAM modbus_up: v%s !!!!\r\n", Version)

	//for count := 0; count < int(config.Tcp_serial[0].Count_node); count++ {
	//	if config.Tcp_serial[0].Set_node[count].Type_par == 3 {
	//		kr.KR_num = 1
	//		kr.Address_data = uint16(config.Tcp_serial[0].Set_node[count].Index_up - 5)
	//	}
	//}

	c := make(chan os.Signal, 1)
	cc := make(chan struct{}) // канал с пустой структурой минимального размера для остановки горутин

	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP)

	for i := 0; i < config.Count_tcp_serial; i++ {
		icc := make(chan inc_req, 3)                            // создадим канал для записи данных в горутину
		go req_tcp_serial(&config.Tcp_serial[i], cc, icc, Buff) // запускаем обработчик канала ввода-вывода
	}

	//<-c // ожидание нажатия Ctrl+C или kill

	cc <- struct{}{} // остановка горутин (минимальная версия по размеру )
	Debug = false    // останавливаем вывод на экран отладочной информации
	fmt.Printf("Stop !!!!!\n\n")
	Log.Printf("Stop !!!!!\n\n")
	time.Sleep(time.Millisecond * 400)
}
