package main

import (
	"crypto/subtle"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"github.com/spf13/viper"
	"net/http"
	"time"
)

//todo response time unix timestamp
type ResponseMessage struct {
	Message      string `json:"message"`
	ResponseTime string `json:"response_time"`
}

type Configurations struct {
	ServerConfig HttpServerConfigurations
	MqttConfig   MqttConfigurations
}

type HttpServerConfigurations struct {
	Port           string
	UserName       string
	Password       string
	MessageTimeout int
}

type MqttConfigurations struct {
	BrokerAddress string
	ClientID      string
	UserName      string
	Password      string
}

var mqttClient mqtt.Client
var mqttMessageCallback = make(chan string)
var http2mqtt *echo.Echo
var configuration Configurations

func main() {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()
	viper.SetConfigType("yml")
	http2mqtt = echo.New()

	http2mqtt.Use(middleware.Logger())
	http2mqtt.Use(middleware.Recover())

	//todo log library change
	http2mqtt.Logger.SetLevel(1)

	if err := viper.ReadInConfig(); err != nil {
		http2mqtt.Logger.Fatal("Error reading config file " + err.Error())
	}

	err := viper.Unmarshal(&configuration)
	if err != nil {
		http2mqtt.Logger.Fatal("Unable to decode into struct, %v", err)
	}

	http2mqtt.Logger.Debug("http port " + configuration.ServerConfig.Port)
	http2mqtt.Logger.Debug("basic auth username ", configuration.ServerConfig.UserName)
	http2mqtt.Logger.Debug("basic auth password ", configuration.ServerConfig.Password)
	http2mqtt.Logger.Debug("mqtt broker address ", configuration.MqttConfig.BrokerAddress)
	http2mqtt.Logger.Debug("mqtt clientID ", configuration.MqttConfig.ClientID)
	http2mqtt.Logger.Debug("mqtt username ", configuration.MqttConfig.UserName)
	http2mqtt.Logger.Debug("mqtt password ", configuration.MqttConfig.Password)

	mqttOptions := mqtt.NewClientOptions().AddBroker(configuration.MqttConfig.BrokerAddress)
	mqttOptions.SetClientID(configuration.MqttConfig.ClientID)
	mqttOptions.SetUsername(configuration.MqttConfig.UserName)
	mqttOptions.SetPassword(configuration.MqttConfig.Password)
	mqttOptions.SetDefaultPublishHandler(mqttMessageHandler)

	mqttClient = mqtt.NewClient(mqttOptions)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		http2mqtt.Logger.Fatal(token.Error())
	}

	http2mqtt.Use(middleware.BasicAuth(func(username, password string, c echo.Context) (bool, error) {
		if subtle.ConstantTimeCompare([]byte(username), []byte(configuration.ServerConfig.UserName)) == 1 &&
			subtle.ConstantTimeCompare([]byte(password), []byte(configuration.ServerConfig.Password)) == 1 {
			return true, nil
		}
		return false, nil
	}))

	http2mqtt.GET("/*", updateMqMessage)
	http2mqtt.Logger.Fatal(http2mqtt.Start(configuration.ServerConfig.Port))
}

// e.GET("/topic/:id", updateMqMessage)
func updateMqMessage(c echo.Context) error {
	responseTime := time.Now()
	http2mqtt.Logger.Debug("/get" + c.Request().URL.EscapedPath())
	http2mqtt.Logger.Debug("/post" + c.Request().URL.EscapedPath())
	if token := mqttClient.Subscribe("/get"+c.Request().URL.EscapedPath(), 2, nil); token.Wait() && token.Error() != nil {
		http2mqtt.Logger.Fatal(token.Error())
	}
	message := c.QueryParam("message")
	http2mqtt.Logger.Info(message)

	mqttClient.Publish("/post"+c.Request().URL.EscapedPath(), 2, false, message)
	select {
	case msg := <-mqttMessageCallback:
		mqttClient.Unsubscribe("/get" + c.Request().URL.EscapedPath())
		return c.JSON(http.StatusOK, ResponseMessage{Message: msg, ResponseTime: time.Since(responseTime).String()})
	case <-time.After(time.Duration(configuration.ServerConfig.MessageTimeout) * time.Millisecond):
		http2mqtt.Logger.Info("timeout  seconds")
		mqttClient.Unsubscribe("/get" + c.Request().URL.EscapedPath())
		return c.JSON(http.StatusGatewayTimeout, ResponseMessage{Message: "timeout", ResponseTime: (time.Duration(configuration.ServerConfig.MessageTimeout) * time.Millisecond).String()})
	}
}

var mqttMessageHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	payload := msg.Payload()
	http2mqtt.Logger.Info(topic)
	http2mqtt.Logger.Info(payload)
	if len(mqttMessageCallback) == 0 {
		mqttMessageCallback <- string(payload)
	} else {
		//todo test required
		http2mqtt.Logger.Info("channel full capacity")
	}
}
