package config

import (
	"os"
	"strings"

	"github.com/go-playground/validator"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
)

type Kafka struct {
	Brokers []string `mapstructure:"brokers" validate:"required"`
}

type Processor struct {
	StoragePath string `mapstructure:"storagePath"`
	Replication int    `mapstructure:"replication"`
	Partitions  int    `mapstructure:"partitions"`
}

type Log struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

type Server struct {
	Port int64 `mapstructure:"port" validate:"required"`
}

type Config struct {
	Server    Server    `mapstructure:"server"`
	Kafka     Kafka     `mapstructure:"kafka"`
	Processor Processor `mapstructure:"processor"`
	Logging   Log       `mapstructure:"logging"`
}

func Read() (*Config, error) {
	viperDefaults()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if len(os.Args) > 1 {
		viper.SetConfigFile(os.Args[1])

		if err := viper.ReadInConfig(); err != nil {
			return nil, err
		}
	}

	var c Config
	if err := bindEnv(c); err != nil {
		return nil, err
	}
	if err := viper.Unmarshal(&c); err != nil {
		return nil, err
	}

	v := validator.New()
	err := v.Struct(&c)
	return &c, err
}

func viperDefaults() {
	viper.SetDefault("server.port", 9175)
}

func bindEnv(v any) error {
	envKeysMap := map[string]any{}
	if err := mapstructure.Decode(v, &envKeysMap); err != nil {
		return err
	}

	keys := envKeys(envKeysMap)
	for _, key := range keys {
		if err := viper.BindEnv(key); err != nil {
			return err
		}
	}
	return nil
}

func envKeys(m map[string]any) []string {
	keys := make([]string, 0)
	for k, v := range m {
		prefix := k

		if vm, isMap := v.(map[string]any); isMap {
			subkeys := envKeys(vm)
			for _, sk := range subkeys {
				keys = append(keys, prefix+"."+sk)
			}
		} else {
			keys = append(keys, prefix)
		}
	}
	return keys
}
