package main

import (
	"github.com/thinkgos/go-iecp5/asdu"
	"github.com/thinkgos/go-iecp5/cs104"
	"log"
	"time"
)

var SRV cs104.Server
var ASD asdu.ASDU

func main() {
	Par := &asdu.Params{CauseSize: 2, CommonAddrSize: 2, InfoObjAddrSize: 3, InfoObjTimeZone: time.UTC}
	SRV := cs104.NewServer(&mysrv{})
	SRV.SetOnConnectionHandler(func(c asdu.Connect) {
		log.Println("on connect")
	})
	SRV.SetConnectionLostHandler(func(c asdu.Connect) {
		log.Println("connect lost")
	})
	SRV.LogMode(true)
	vari := asdu.VariableStruct{Number: 1, IsSequence: false}
	coa := asdu.CauseOfTransmission{Cause: 3, IsTest: false, IsNegative: false}
	id := asdu.Identifier{Type: 1, Variable: vari, Coa: coa, OrigAddr: 1, CommonAddr: 1}
	ASD := asdu.NewASDU(Par, id)
	SRV.SetParams(ASD.Params)
	ASD.Init_par()
	SRV.ListenAndServer(":2404")
}

type mysrv struct{}

func (sf *mysrv) InterrogationHandler(c asdu.Connect, asduPack *asdu.ASDU, qoi asdu.QualifierOfInterrogation) error {
	log.Println("qoi", qoi)
	asduPack.SendReplyMirror(c, asdu.ActivationCon)

	send_parad(sf, c, asduPack)

	asduPack.SendReplyMirror(c, asdu.ActivationTerm)
	return nil
}

func send_parad(sf *mysrv, c asdu.Connect, asduPack *asdu.ASDU) {
	var rt asdu.SinglePointInfo
	rt.Ioa = 200
	rt.Time = time.Now()
	rt.Qds = 1
	rt.Value = true

	//for {
	for i := 0; i < 4; i++ {
		asdu.MeasuredValueFloatCP56Time2a(c, asdu.CauseOfTransmission{Cause: asdu.Spontaneous}, 1, asdu.Par_send[i].Mek_104)
	}
	err := asdu.Single(c, false, asdu.CauseOfTransmission{Cause: asdu.Spontaneous}, asduPack.CommonAddr, rt)
	if err != nil {
		log.Println("falied", err)
	} else {
		log.Println("success", err)
	}
	if rt.Value {
		rt.Value = false
	} else {
		rt.Value = true
	}
	//time.Sleep(time.Second * 1)

	//}
}

func (sf *mysrv) CounterInterrogationHandler(asdu.Connect, *asdu.ASDU, asdu.QualifierCountCall) error {
	return nil
}
func (sf *mysrv) ReadHandler(c asdu.Connect, asdu1 *asdu.ASDU, io asdu.InfoObjAddr) error {
	for i := 0; i < 4; i++ {
		if asdu.Par_send[i].Mek_104.Ioa == io {
			asdu.MeasuredValueFloatCP56Time2a(c, asdu.CauseOfTransmission{Cause: asdu.Request}, 1, asdu.Par_send[i].Mek_104)
		}
	}
	return nil
}
func (sf *mysrv) ClockSyncHandler(asdu.Connect, *asdu.ASDU, time.Time) error { return nil }
func (sf *mysrv) ResetProcessHandler(asdu.Connect, *asdu.ASDU, asdu.QualifierOfResetProcessCmd) error {
	return nil
}
func (sf *mysrv) DelayAcquisitionHandler(asdu.Connect, *asdu.ASDU, uint16) error { return nil }
func (sf *mysrv) ASDUHandler(asdu.Connect, *asdu.ASDU) error                     { return nil }
