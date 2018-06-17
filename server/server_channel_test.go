package server

import "testing"

func Test_ChannelOpen_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultServerConfig())
	_, err := sc.client.Channel()
	if err != nil {
		t.Fatal(err)
	}
}
