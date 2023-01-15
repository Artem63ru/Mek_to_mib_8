package main

import (
	"log"
	"math/rand"
	"time"

	"github.com/thinkgos/go-iecp5/asdu"
	"github.com/thinkgos/go-iecp5/cs104"
)

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
	vari := asdu.VariableStruct{Number: 10, IsSequence: false}
	coa := asdu.CauseOfTransmission{Cause: 1, IsTest: false, IsNegative: false}
	id := asdu.Identifier{Type: 1, Variable: vari, Coa: coa, OrigAddr: 0, CommonAddr: 1}
	asd := asdu.NewASDU(Par, id)
	SRV.SetParams(asd.Params)

	//	srv.SetParams(asdu.Params{2, 1, 1, 1, time.UTC})
	// go func() {
	// 	time.Sleep(time.Second * 20)
	// 	log.Println("try ooooooo", err)
	// 	err := srv.Close()
	// 	log.Println("ooooooo", err)
	// }()
	SRV.ListenAndServer(":2404")
}

type mysrv struct{}

func (sf *mysrv) InterrogationHandler(c asdu.Connect, asduPack *asdu.ASDU, qoi asdu.QualifierOfInterrogation) error {
	log.Println("qoi", qoi)
	asduPack.SendReplyMirror(c, asdu.ActivationCon)
	var rt asdu.SinglePointInfo
	rt.Ioa = 200
	rt.Time = time.Now()
	rt.Qds = 1
	rt.Value = true
	var vale asdu.MeasuredValueFloatInfo
	vale.Ioa = 4001
	vale.Qds = 1
	vale.Value = 150

	err := asdu.Single(c, false, asdu.CauseOfTransmission{Cause: asdu.InterrogatedByStation}, asduPack.CommonAddr,
		rt)
	if err != nil {
		// log.Println("falied")
	} else {
		// log.Println("success")
	}
	go func() {
		for {
			err := asdu.Single(c, false, asdu.CauseOfTransmission{Cause: asdu.Spontaneous}, asduPack.CommonAddr,
				rt)
			asdu.MeasuredValueFloatCP56Time2a(c, asdu.CauseOfTransmission{Cause: asdu.Spontaneous}, asduPack.CommonAddr,
				vale)
			if err != nil {
				log.Println("falied", err)
			} else {
				log.Println("success", err)
			}
			vale.Value = rand.Float32()
			vale.Time = time.Now()
			if rt.Value {
				rt.Value = false
			} else {
				rt.Value = true
			}
			time.Sleep(time.Second * 1)
		}
	}()
	asduPack.SendReplyMirror(c, asdu.ActivationTerm)
	return nil
}
func (sf *mysrv) CounterInterrogationHandler(asdu.Connect, *asdu.ASDU, asdu.QualifierCountCall) error {
	return nil
}
func (sf *mysrv) ReadHandler(asdu.Connect, *asdu.ASDU, asdu.InfoObjAddr) error { return nil }
func (sf *mysrv) ClockSyncHandler(asdu.Connect, *asdu.ASDU, time.Time) error   { return nil }
func (sf *mysrv) ResetProcessHandler(asdu.Connect, *asdu.ASDU, asdu.QualifierOfResetProcessCmd) error {
	return nil
}
func (sf *mysrv) DelayAcquisitionHandler(asdu.Connect, *asdu.ASDU, uint16) error { return nil }
func (sf *mysrv) ASDUHandler(asdu.Connect, *asdu.ASDU) error                     { return nil }
