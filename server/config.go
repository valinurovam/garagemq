package server

type ServerConfig struct {
	Users []ConfigUser
	Tcp   TcpConfig
}

type ConfigUser struct {
	Username string
	Password string
}

type TcpConfig struct {
	Nodelay      bool
	ReadBufSize  int `yaml:"readBufSize"`
	WriteBufSize int `yaml:"writeBufSize"`
}
