package server

import "testing"

func Test_Confirm_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	err := ch.Confirm(false)

	if err != nil {
		t.Fatal(err)
	}

	channel := getServerChannel(sc, 1)
	if channel.confirmMode == false {
		t.Fatal("Channel non confirm mode")
	}
}
