package config

import (
	"io/ioutil"
	"log"

	yaml "gopkg.in/yaml.v2"
)

//Wraper yaml
type Wraper struct {
	Version     string      `yaml:"version"`
	Source      Conn        `yaml:"src"`
	Destination Conn        `yaml:"dst"`
	Table       []TableInfo `yaml:"table"`
}

//Conn conn
type Conn struct {
	Host     string `yaml:"host"`
	User     string `yaml:"user"`
	Pass     string `yaml:"pwd"`
	Database string `yaml:"dbname"`
	Port     string `yaml:"port"`
}

//TableInfo info
type TableInfo struct {
	Name    string   `yaml:"name"`
	Rebuild bool     `yaml:"rebuild"`
	Batch   int64    `yaml:"batch"`
	Where   []string `yaml:"where"`
}

var W = Wraper{}

func InitConfig() {
	ret, err := ioutil.ReadFile("./config.yaml")
	if err != nil {
		log.Println(err)
	}
	yaml.Unmarshal(ret, &W)
}
