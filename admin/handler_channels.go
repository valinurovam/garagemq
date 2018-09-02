package admin

//import (
//	"net/http"
//
//	"github.com/valinurovam/garagemq/server"
//)
//
//type ChannelsHandler struct {
//	amqpServer *server.Server
//}
//
//type ChannelsResponse struct {
//	Items []*Channel `json:"items"`
//}
//
//type Channel struct {
//	Channel string `json:"channel"`
//	Vhost   string `json:"vhost"`
//	User    string `json:"user"`
//	Qos     string `json:"qos"`
//}
//
//func NewChannelsHandler(amqpServer *server.Server) http.Handler {
//	return &ChannelsHandler{amqpServer: amqpServer}
//}
//
//func (h *ChannelsHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
//	response := &ChannelsResponse{}
//	for _, conn := range h.amqpServer.GetConnections() {
//		for id, ch := range conn.GetChannels() {
//
//			response.Items = append(
//				response.Items,
//				&Channel{
//					Channel: conn.GetRemoteAddr().String() + string(id),
//				},
//			)
//		}
//	}
//
//	JSONResponse(resp, response, 200)
//}
