package feishu

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"net/http"
	"strings"
	"time"
)

// DefaultBaseURL 飞书（中国版）API 域名；Lark 国际版为 https://open.larksuite.com
const DefaultBaseURL = "https://open.feishu.cn"

type Sdk struct {
	appID     string
	appSecret string
	token     string
	baseURL   string
	client    http.Client
}

func NewSDK(appID string, appSecret string, baseURL string) *Sdk {
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	// token 惰性获取：首次 API 调用时刷新（见 call），成功后定时续期，
	// 避免启动时网络故障导致构造无限阻塞
	return &Sdk{
		appID:     appID,
		appSecret: appSecret,
		baseURL:   baseURL,
		client:    http.Client{},
	}
}

type batchGetIDResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		EmailUsers map[string][]struct {
			OpenId string `json:"open_id"`
			UserId string `json:"user_id"`
		} `json:"email_users"`
		EmailsNotExist []string `json:"emails_not_exist"`
		MobileUsers    map[string][]struct {
			OpenId string `json:"open_id"`
			UserId string `json:"user_id"`
		} `json:"mobile_users"`
		MobilesNotExist []string `json:"mobiles_not_exist"`
	} `json:"data"`
}

// BatchGetID https://open.feishu.cn/document/ukTMukTMukTM/uUzMyUjL1MjM14SNzITN
func (s *Sdk) BatchGetID(emails []string) (map[string]string, error) {
	if len(emails) == 0 {
		return nil, errors.New("at least 1 email")
	}
	if len(emails) > 50 {
		return nil, errors.New("at most 50 emails")
	}

	api := s.baseURL + "/open-apis/user/v1/batch_get_id?"
	for _, email := range emails {
		api += "emails=" + email + "&"
	}
	api = api[:len(api)-1]
	var response batchGetIDResponse
	err := s.get(api, s.token, &response)
	if err != nil {
		return nil, err
	}

	if response.Code != 0 {
		return nil, errors.New(fmt.Sprintf("code: %d, err: %s", response.Code, response.Msg))
	}

	res := make(map[string]string)
	for k, vv := range response.Data.EmailUsers {
		for _, v := range vv {
			res[k] = v.OpenId
		}
	}
	return res, nil
}

type sendCardRequest struct {
	ReceiveID string `json:"receive_id"`
	MsgType   string `json:"msg_type"`
	Content   string `json:"content"`
}

type sendCardResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		MessageID string `json:"message_id"`
	} `json:"data"`
}

// SendMessage 以应用身份发送卡片消息到群聊，返回 message_id
// https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/im-v1/message/create
func (s *Sdk) SendMessage(chatID string, content string) (string, error) {
	request := sendCardRequest{
		ReceiveID: chatID,
		MsgType:   "interactive",
		Content:   content,
	}
	var response sendCardResponse
	err := s.post(s.baseURL+"/open-apis/im/v1/messages?receive_id_type=chat_id", s.token, request, &response)
	if err != nil {
		return "", err
	}
	if response.Code != 0 {
		return "", fmt.Errorf("send message failed, code: %d, err: %s", response.Code, response.Msg)
	}
	return response.Data.MessageID, nil
}

// PatchCard 更新应用已发送的卡片消息，要求卡片 config.update_multi 为 true
// 仅可更新 14 天内发送的消息
// https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/im-v1/message/patch
func (s *Sdk) PatchCard(messageID string, content string) error {
	request := map[string]string{"content": content}
	var response sendCardResponse
	err := s.call("PATCH", s.baseURL+"/open-apis/im/v1/messages/"+messageID, s.token, request, &response)
	if err != nil {
		return err
	}
	if response.Code != 0 {
		return fmt.Errorf("patch card failed, code: %d, err: %s", response.Code, response.Msg)
	}
	return nil
}

type tokenRequest struct {
	AppID     string `json:"app_id"`
	AppSecret string `json:"app_secret"`
}

type tokenResponse struct {
	Code              int    `json:"code"`
	Msg               string `json:"msg"`
	TenantAccessToken string `json:"tenant_access_token"`
	Expire            int    `json:"expire"`
}

// TenantAccessToken https://open.feishu.cn/document/ukTMukTMukTM/uIjNz4iM2MjLyYzM
func (s *Sdk) TenantAccessToken() (*tokenResponse, error) {
	request := tokenRequest{
		AppID:     s.appID,
		AppSecret: s.appSecret,
	}
	var response tokenResponse
	err := s.post(s.baseURL+"/open-apis/auth/v3/tenant_access_token/internal/", s.token, request, &response)
	if err != nil {
		return nil, err
	}

	if response.Code != 0 {
		return nil, errors.New(fmt.Sprintf("code: %d, err: %s", response.Code, response.Msg))
	}

	return &response, nil
}

// wired response:
// response of success
//
//	{
//	   "Extra": null,
//	   "StatusCode": 0,
//	   "StatusMessage": "success"
//	}
//
// response of failure
//
//	{
//	   "code": 99991300,
//	   "msg": "invalid request body: not json, invalid character '\\n' in string literal"
//	}
type webhookV2Response struct {
	StatusCode    int    `json:"StatusCode"`
	StatusMessage string `json:"StatusMessage"`
	Code          int    `json:"code"`
	Msg           string `json:"msg"`
}

func (s *Sdk) WebhookV2(webhook string, body io.Reader) error {
	logrus.Debug(webhook, body)
	req, err := http.NewRequest("POST", webhook, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	do, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer do.Body.Close()

	var resp webhookV2Response
	err = json.NewDecoder(do.Body).Decode(&resp)
	if err != nil {
		return err
	}
	logrus.Debug(resp)

	if resp.Code != 0 {
		return errors.New(fmt.Sprintf("code: %d, err: %s", resp.Code, resp.Msg))
	}

	return nil
}

func (s *Sdk) get(url string, auth string, responseBody interface{}) error {
	return s.call("GET", url, auth, nil, responseBody)
}

func (s *Sdk) post(url string, auth string, requestBody, responseBody interface{}) error {
	return s.call("POST", url, auth, requestBody, responseBody)
}

func (s *Sdk) call(method string, url string, auth string, requestBody, responseBody interface{}) error {
	logrus.Debugf("%s %s with %v", method, url, requestBody)

	// token 惰性刷新（跳过 token 接口自身，防递归）；
	// 并发下的重复刷新只会多请求一次 token 接口，无害
	if s.appID != "" && s.token == "" && !strings.Contains(url, "tenant_access_token") {
		s.refreshToken()
	}

	var body io.Reader
	if requestBody != nil {
		bs, err := json.Marshal(requestBody)
		if err != nil {
			return err
		}
		body = bytes.NewReader(bs)
	}

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	if auth != "" {
		req.Header.Set("Authorization", "Bearer "+auth)
	}

	do, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer do.Body.Close()

	err = json.NewDecoder(do.Body).Decode(&responseBody)
	if err != nil {
		return err
	}

	logrus.Debug(responseBody)

	return nil
}

func (s *Sdk) refreshToken() {
	response, err := s.TenantAccessToken()
	if err != nil {
		// 不无限重试：置空 token，下次 API 调用时惰性重试（见 call）
		logrus.Errorf("refresh token failed, %v", err)
		s.token = ""
		return
	}
	s.token = response.TenantAccessToken

	// https://open.feishu.cn/document/ukTMukTMukTM/uIjNz4iM2MjLyYzM
	// Token 有效期为 2 小时，在此期间调用该接口 token 不会改变。当 token 有效期小于 30 分的时候，再次请求获取 token 的时候，会生成一个新的 token，与此同时老的 token 依然有效。
	// 在过期前 1 分钟刷新
	time.AfterFunc(time.Second*time.Duration(response.Expire-60), func() {
		s.refreshToken()
	})
}
