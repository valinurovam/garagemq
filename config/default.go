package config

func defaultConfig() *Config {
	return &Config{
		Proto: "amqp-rabbit",
		Users: []User{
			{
				Username: "guest",
				Password: "084e0343a0486ff05530df6c705c8bb4", // guest md5 hash
			},
		},
		TCP: TCPConfig{
			IP:           "0.0.0.0",
			Port:         "5672",
			Nodelay:      false,
			ReadBufSize:  196608,
			WriteBufSize: 196608,
		},
		Admin: AdminConfig{
			IP:   "0.0.0.0",
			Port: "15672",
		},
		Queue: Queue{
			ShardSize:        65536,
			MaxMessagesInRam: 131072,
		},
		Db: Db{
			DefaultPath: "db",
			Engine:      "badger",
		},
		Vhost: Vhost{
			DefaultPath: "/",
		},
		Security: Security{
			PasswordCheck: "md5",
		},
		Connection: Connection{
			ChannelsMax:  4096,
			FrameMaxSize: 65536,
		},
	}
}
