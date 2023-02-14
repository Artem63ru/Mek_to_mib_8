// Package asdu provides the OSI presentation layer.
package asdu

import (
	modbus_mk "MEK104/modbus"
	"fmt"
	"io"
	"math/bits"
	"time"
)

// ASDUSizeMax asdu max size
const (
	ASDUSizeMax = 249
)

// ASDU format
//       | data unit identification | information object <1..n> |
//
//       | <------------  data unit identification ------------>|
//       | typeID | variable struct | cause  |  common address  |
// bytes |    1   |      1          | [1,2]  |      [1,2]       |
//       | <------------  information object ------------------>|
//       | object address | element set  |  object time scale   |
// bytes |     [1,2,3]    |              |                      |

var (
	// ParamsNarrow is the smallest configuration.
	ParamsNarrow = &Params{CauseSize: 1, CommonAddrSize: 1, InfoObjAddrSize: 1, InfoObjTimeZone: time.UTC}
	// ParamsWide is the largest configuration.
	ParamsWide = &Params{CauseSize: 2, CommonAddrSize: 2, InfoObjAddrSize: 3, InfoObjTimeZone: time.UTC}

	Buff = [][]Buffer{}
)

// Params 定义了ASDU相关特定参数
// See companion standard 101, subclass 7.1.
type Params struct {
	// cause of transmission, 传输原因字节数
	// The standard requires "b" in [1, 2].
	// Value 2 includes/activates the originator address.
	CauseSize int
	// Originator Address [1, 255] or 0 for the default.
	// The applicability is controlled by Params.CauseSize.
	OrigAddress OriginAddr
	// size of ASDU common address， ASDU 公共地址字节数
	// 应用服务数据单元公共地址的八位位组数目,公共地址是站地址
	// The standard requires "a" in [1, 2].
	CommonAddrSize int

	// size of ASDU information object address. 信息对象地址字节数
	// The standard requires "c" in [1, 3].
	InfoObjAddrSize int

	// InfoObjTimeZone controls the time tag interpretation.
	// The standard fails to mention this one.
	InfoObjTimeZone *time.Location
}

// Описание структуры набора аналоговых параметров для обмена
type BD_params_float struct {
	// порядковый номер параметра
	ID int
	// Наименование параметра
	Name string
	// Описание параметра /адрес/значение/регистры передачи/время для протокола МЭК 104
	Mek_104 MeasuredValueFloatInfo
	// Адрес параметра в Modbus устройтвые источника / ноемр регистра
	Mod_adress int
	// Время последнего изменения - опционально
	Uptime time.Time
	// Флаг изменения сигнала для спародической передачи
	Up_Val bool
	// Верхний предел аналога
	Ai_hi float32
	// нижний предел аналога
	Ai_low float32
	// предыдущее значение параметра
	Old_Value float32
}

// Описание структуры набора дискретных параметров для обмена
type BD_params_singl struct {
	// порядковый номер параметра
	ID int
	// Наименование параметра
	Name string
	// Описание параметра /адрес/значение/регистры передачи/время для протокола МЭК 104
	Mek_104 SinglePointInfo
	// Адрес параметра в Modbus устройтвые источника / ноемр регистра
	Mod_adress int
	// Время последнего изменения - опционально
	Uptime time.Time
	// Флаг изменения сигнала для спародической передачи
	Up_Val bool
}

// Описание структуры набора дискретных параметров для обмена
type BD_params_KR struct {
	// порядковый номер параметра
	ID int
	// Наименование параметра
	Name string
	// Описание параметра /адрес/значение/регистры передачи/время для протокола МЭК 104 Открыть
	Mek_104_On SingleCommandInfo
	// Описание параметра /адрес/значение/регистры передачи/время для протокола МЭК 104 Закрыть
	Mek_104_Off SingleCommandInfo
	// Команда на сброс формируемых ошибок
	CRFX SingleCommandInfo
	// Адрес параметра в Modbus устройтвые источника / ноемр регистра
	Mod_adress int
	// Время последнего изменения - опционально
	Uptime time.Time
	// Флаг изменения сигнала для спародической передачи
	Up_Val bool
	//Конечник открытия
	KR_ON bool
	//Конечник закрытия
	KR_OF bool
	// Команда открытия
	COM_ON bool
	// Команда закрытия
	COM_OF bool
	//Таймер перестановки крана
	Tim_com *time.Timer
	// Время перестановки крана
	time_in_seconds float32
	FDSX            SinglePointInfo //	Информация о сработке одновременно двух противоположных концевиков.
	FONX            SinglePointInfo //	Информация о том, что после подачи команды на открытие не пришёл сигнал с концевика открыт (IONX) за установленное время (STSX).
	FOFX            SinglePointInfo //	Информация о том, что после подачи команды на закрытие не пришёл сигнал с концевика закрыт (IOFX) за установленное время (STSX).
	FFON            SinglePointInfo //	Информация об открытии (пропадание концевика закрыт) ИМ без управляющего воздействия
	FFOF            SinglePointInfo //	Информация о закрытии (пропадание концевика открыт) ИМ без управляющего воздействия
	SolON           SinglePointInfo //	Информация об соленойде открытии КЗ или обрыв
	SolOF           SinglePointInfo //	Информация о закрытии соленойде  КЗ или обрыв
	Sol_Com_ON      SinglePointInfo //	Информация об соленойде команды открытии КЗ или обрыв
	Sol_COM_OF      SinglePointInfo //	Информация о соленойде команды закрытии КЗ или обрыв

	Done chan bool

	Com_Kran

	IONX_prvs bool
	IOFX_prvs bool
}

type Com_Kran interface {
	C_ON(done <-chan bool) bool
	C_OF(done <-chan bool) bool
	FCom_OF(value bool) bool
}

// Буфер для хранения данных
type Buffer struct {
	// Запись о параметре
	parameter []BD_params_float
}

// Структура буфера для управления каранами для передачи в МЭК
type Bufer_KR struct {
	KR_sel      bool
	Send_cancel bool
	CMD         bool
	Num_chanel  int
}

// Функция открытия крана по таймеры
func (k *BD_params_KR) C_ON(done <-chan bool) bool {
	if k.KR_OF && k.COM_ON {
		fmt.Print("Выставил команду открыть", k.ID)
		modbus_mk.Com_tcp_serial(true, false, k.ID)
		k.Tim_com = time.NewTimer(120 * time.Second) // таймер перестановки

		select {
		case <-done:
			k.Tim_com.Stop()
			fmt.Printf("Прервано закрытие %d \n", k.time_in_seconds)
			k.COM_ON = false
			modbus_mk.Com_tcp_serial(false, true, k.ID)
			break
		case <-k.Tim_com.C:
			if !k.COM_ON {
				fmt.Printf("Кран ошибка открытия не открылся %d \n", k.time_in_seconds)
			}
			k.COM_ON = false
			k.FONX.Value = true
			modbus_mk.Com_tcp_serial(false, true, k.ID)
			// If main() finishes before the 60 second timer, we won't get here
			fmt.Printf("не включился за заданное время %d \n", k.Mek_104_On.Ioa)
			break
		}

	} else {
		k.COM_ON = false
		k.Mek_104_On.Value = false
		fmt.Printf("Ошибка открытия %d \n", k.time_in_seconds)
	}
	return k.COM_ON
}

// Функция закрытия крана по таймеру
func (k *BD_params_KR) C_OF(done <-chan bool) bool {
	if k.KR_ON && k.COM_OF {
		fmt.Print("Выставил команду")
		modbus_mk.Com_tcp_serial(true, false, k.ID+1)
		k.Tim_com = time.NewTimer(120 * time.Second) // таймер перестановки

		select {
		case <-done:
			k.Tim_com.Stop()
			fmt.Printf("Прервано закрытие %d \n", k.time_in_seconds)
			k.COM_OF = false

			modbus_mk.Com_tcp_serial(false, true, k.ID+1)
			break
		case <-k.Tim_com.C:
			if !k.COM_OF {
				fmt.Printf("Кран ошибка закрытия не закрылся %d \n", k.time_in_seconds)
			}
			k.COM_OF = false
			modbus_mk.Com_tcp_serial(false, true, k.ID+1)
			// If main() finishes before the 60 second timer, we won't get here
			k.FOFX.Value = true
			fmt.Printf("Кран не отключился за заданное время %d \n", k.Mek_104_Off.Ioa)
			break
		}

	} else {
		k.COM_OF = false
		k.Mek_104_Off.Value = false
		fmt.Printf("Ошибка закрытия %d \n", k.time_in_seconds)
	}
	return k.COM_OF
}

// Функция открытия и закрытия крана с отслеживанием концевиков
func (k BD_params_KR) FCom_OF(value bool) bool {
	if value {
		if !k.COM_OF {
			k.COM_OF = true
			//Buff_KR[i].Mek_104.Value = true
			done := make(chan bool, 1)
			k.Done = done
			go k.C_OF(done) // запуск команды
		}
	} else {
		if k.COM_OF {
			k.Done <- true
			fmt.Println("Кран сброшен ", k.COM_OF)
			k.COM_OF = false

		}
		k.Up_Val = true
	}
	go func() {
		for {
			if k.KR_OF && k.COM_OF {
				k.Done <- true
				fmt.Println("Кран Закрыт ", k.COM_OF)
				k.COM_OF = false

			}
			if !k.COM_OF {
				break
			}
		}

	}()
	return k.COM_OF
}

// Valid returns the validation result of params.
func (sf Params) Valid() error {
	if (sf.CauseSize < 1 || sf.CauseSize > 2) ||
		(sf.CommonAddrSize < 1 || sf.CommonAddrSize > 2) ||
		(sf.InfoObjAddrSize < 1 || sf.InfoObjAddrSize > 3) ||
		(sf.InfoObjTimeZone == nil) {
		return ErrParam
	}
	return nil
}

// ValidCommonAddr returns the validation result of a station common address.
func (sf Params) ValidCommonAddr(addr CommonAddr) error {
	if addr == InvalidCommonAddr {
		return ErrCommonAddrZero
	}
	if bits.Len(uint(addr)) > sf.CommonAddrSize*8 {
		return ErrCommonAddrFit
	}
	return nil
}

// IdentifierSize return the application service data unit identifies size
func (sf Params) IdentifierSize() int {
	return 2 + int(sf.CauseSize) + int(sf.CommonAddrSize)
}

// Identifier the application service data unit identifies.
type Identifier struct {
	// type identification, information content
	Type TypeID
	// Variable is variable structure qualifier
	Variable VariableStruct
	// cause of transmission submission category
	Coa CauseOfTransmission
	// Originator Address [1, 255] or 0 for the default.
	// The applicability is controlled by Params.CauseSize.
	OrigAddr OriginAddr
	// CommonAddr is a station address. Zero is not used.
	// The width is controlled by Params.CommonAddrSize.
	// See companion standard 101, subclass 7.2.4.
	CommonAddr CommonAddr // station address 公共地址是站地址
}

// String 返回数据单元标识符的信息,例： "TypeID Cause OrigAddr@CommonAddr"
func (id Identifier) String() string {
	if id.OrigAddr == 0 {
		return fmt.Sprintf("%s %s @%d", id.Type, id.Coa, id.CommonAddr)
	}
	return fmt.Sprintf("%s %s %d@%d ", id.Type, id.Coa, id.OrigAddr, id.CommonAddr)
}

// ASDU (Application Service Data Unit) is an application message.
type ASDU struct {
	*Params
	Identifier
	infoObj   []byte            // information object serial
	bootstrap [ASDUSizeMax]byte // prevents Info malloc
}

// NewEmptyASDU new empty asdu with special params
func NewEmptyASDU(p *Params) *ASDU {
	a := &ASDU{Params: p}
	lenDUI := a.IdentifierSize()
	a.infoObj = a.bootstrap[lenDUI:lenDUI]
	return a
}

// NewASDU new asdu with special params and identifier
func NewASDU(p *Params, identifier Identifier) *ASDU {
	a := NewEmptyASDU(p)
	a.Identifier = identifier
	return a
}

// Clone deep clone asdu
func (sf *ASDU) Clone() *ASDU {
	r := NewASDU(sf.Params, sf.Identifier)
	r.infoObj = append(r.infoObj, sf.infoObj...)
	return r
}

// SetVariableNumber See companion standard 101, subclass 7.2.2.
func (sf *ASDU) SetVariableNumber(n int) error {
	if n >= 128 {
		return ErrInfoObjIndexFit
	}
	sf.Variable.Number = byte(n)
	return nil
}

// Respond returns a new "responding" ASDU which addresses "initiating" u.
//func (u *ASDU) Respond(t TypeID, c Cause) *ASDU {
//	return NewASDU(u.Params, Identifier{
//		CommonAddr: u.CommonAddr,
//		OrigAddr:   u.OrigAddr,
//		Type:       t,
//		Cause:      c | u.Cause&TestFlag,
//	})
//}

// Reply returns a new "responding" ASDU which addresses "initiating" addr with a copy of Info.
func (sf *ASDU) Reply(c Cause, addr CommonAddr) *ASDU {
	sf.CommonAddr = addr
	r := NewASDU(sf.Params, sf.Identifier)
	r.Coa.Cause = c
	r.infoObj = append(r.infoObj, sf.infoObj...)
	return r
}

// SendReplyMirror send a reply of the mirror request but cause different
func (sf *ASDU) SendReplyMirror(c Connect, cause Cause) error {
	r := NewASDU(sf.Params, sf.Identifier)
	r.Coa.Cause = cause
	r.infoObj = append(r.infoObj, 0, 0, 0)
	r.infoObj = append(r.infoObj, sf.infoObj...)
	return c.Send(r)
}

// Моя функция для показа 100
func (sf *ASDU) SendReply_CMD(c Connect, cause Cause) error {
	r := NewASDU(sf.Params, sf.Identifier)
	r.Coa.Cause = cause
	//	iot.Ioa
	r.infoObj = append(r.infoObj)
	//r.infoObj = append(r.infoObj, 0, 0, 0)
	r.infoObj = append(r.infoObj, sf.infoObj...)
	return c.Send(r)
}

// Моя функция для показа M_SP_NA_1
func (sf *ASDU) SendReply_M_SP_NA_1(c Connect, cause Cause) error {
	r := NewASDU(sf.Params, sf.Identifier)
	r.Type = M_SP_NA_1
	r.Coa.Cause = 20
	r.infoObj = append(r.infoObj, sf.infoObj...)
	return c.Send(r)
}

// Моя функция для показа M_ME_NB_1
func (sf *ASDU) SendReply_M_ME_NB_1(c Connect, cause Cause) error {
	r := NewASDU(sf.Params, sf.Identifier)
	r.Type = M_ME_NB_1
	r.Coa.Cause = 20
	r.infoObj = append(r.infoObj, sf.infoObj...)
	return c.Send(r)
}

// MarshalBinary honors the encoding.BinaryMarshaler interface.
func (sf *ASDU) MarshalBinary() (data []byte, err error) {
	//var n = 0
	switch {
	case sf.Coa.Cause == Unused:
		return nil, ErrCauseZero
	case !(sf.CauseSize == 1 || sf.CauseSize == 2):
		return nil, ErrParam
	case sf.CauseSize == 1 && sf.OrigAddr != 0:
		return nil, ErrOriginAddrFit
	case sf.CommonAddr == InvalidCommonAddr:
		return nil, ErrCommonAddrZero
	case !(sf.CommonAddrSize == 1 || sf.CommonAddrSize == 2):
		return nil, ErrParam
	case sf.CommonAddrSize == 1 && sf.CommonAddr != GlobalCommonAddr && sf.CommonAddr >= 255:
		return nil, ErrParam
	}

	raw := sf.bootstrap[:(sf.IdentifierSize() + len(sf.infoObj))]
	raw[0] = byte(sf.Type)
	raw[1] = sf.Variable.Value()
	raw[2] = sf.Coa.Value()
	offset := 3
	if sf.CauseSize == 2 {
		raw[offset] = byte(sf.OrigAddr)
		offset++
	}
	if sf.CommonAddrSize == 1 {
		if sf.CommonAddr == GlobalCommonAddr {
			raw[offset] = 255
		} else {
			raw[offset] = byte(sf.CommonAddr)
		}
	} else { // 2
		raw[offset] = byte(sf.CommonAddr)
		offset++
		raw[offset] = byte(sf.CommonAddr >> 8)
	}

	return raw, nil
}

// UnmarshalBinary honors the encoding.BinaryUnmarshaler interface.
// ASDUParams must be set in advance. All other fields are initialized.
func (sf *ASDU) UnmarshalBinary(rawAsdu []byte) error {
	if !(sf.CauseSize == 1 || sf.CauseSize == 2) ||
		!(sf.CommonAddrSize == 1 || sf.CommonAddrSize == 2) {
		return ErrParam
	}

	// rawAsdu unit identifier size check
	lenDUI := sf.IdentifierSize()
	if lenDUI > len(rawAsdu) {
		return io.EOF
	}

	// parse rawAsdu unit identifier
	sf.Type = TypeID(rawAsdu[0])
	sf.Variable = ParseVariableStruct(rawAsdu[1])
	sf.Coa = ParseCauseOfTransmission(rawAsdu[2])
	if sf.CauseSize == 1 {
		sf.OrigAddr = 0
	} else {
		sf.OrigAddr = OriginAddr(rawAsdu[3])
	}
	if sf.CommonAddrSize == 1 {
		sf.CommonAddr = CommonAddr(rawAsdu[lenDUI-1])
		if sf.CommonAddr == 255 { // map 8-bit variant to 16-bit equivalent
			sf.CommonAddr = GlobalCommonAddr
		}
	} else { // 2
		sf.CommonAddr = CommonAddr(rawAsdu[lenDUI-2]) | CommonAddr(rawAsdu[lenDUI-1])<<8
	}
	// information object
	sf.infoObj = append(sf.bootstrap[lenDUI:lenDUI], rawAsdu[lenDUI:]...)
	return sf.fixInfoObjSize()
}

// fixInfoObjSize fix information object size
func (sf *ASDU) fixInfoObjSize() error {
	// fixed element size
	objSize, err := GetInfoObjSize(sf.Type)
	if err != nil {
		return err
	}

	var size int
	// read the variable structure qualifier
	if sf.Variable.IsSequence {
		size = sf.InfoObjAddrSize + int(sf.Variable.Number)*objSize
	} else {
		size = int(sf.Variable.Number) * (sf.InfoObjAddrSize + objSize)
	}

	switch {
	case size == 0:
		return ErrInfoObjIndexFit
	case size > len(sf.infoObj):
		return io.EOF
	case size < len(sf.infoObj): // not explicitly prohibited
		sf.infoObj = sf.infoObj[:size]
	}

	return nil
}

// Буферная передача массива параметров на верх
func Transfer_buff(c Connect, par []BD_params_float) {
	for i := 0; i < len(par); i++ {
		MeasuredValueFloatCP56Time2a(c, CauseOfTransmission{Cause: Spontaneous}, 1, par[i].Mek_104)
	}
}
