package config

var Host string = "0.0.0.0"
var Port int = 6363
var KeysLimit int = 5

var EvictionStrategy string = "simple-first"
var AOFFile string = "./gredis-master.aof"
