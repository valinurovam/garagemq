package server

import (
	"testing"

	"github.com/valinurovam/garagemq/config"
)

func Test_Connection_Success(t *testing.T) {
	sc, err := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	if err != nil {
		t.Fatal(err)
	}
}

func Test_Connection_FailedVhostAccess(t *testing.T) {
	cfg := getDefaultTestConfig()
	cfg.srvConfig.Vhost.DefaultPath = "test"
	sc, err := getNewSC(cfg)
	defer sc.clean()
	if err == nil {
		t.Fatal("Expected no access to vhost error")
	}
}

func Test_Connection_Failed_WhenWrongAuth(t *testing.T) {
	cfg := getDefaultTestConfig()
	cfg.srvConfig.Users = []config.User{
		{
			Username: "guest",
			Password: "guest?",
		},
	}
	sc, err := getNewSC(cfg)
	defer sc.clean()
	if err == nil {
		t.Fatal("Expected auth error")
	}
}
