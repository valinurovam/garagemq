package server


type ServerConfig struct {
	Users []ConfigUser
}

type ConfigUser struct {
	Username string
	Password string
}