package feishu

import (
	"encoding/json"
	"github.com/davecgh/go-spew/spew"
	"github.com/prometheus/alertmanager/template"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/xujiahua/alertmanager-webhook-feishu/config"
	"github.com/xujiahua/alertmanager-webhook-feishu/model"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func getConf() *config.Config {
	conf, err := config.Load("../config.yml")
	if err != nil {
		panic(err)
	}
	return conf
}

func getBotConf() *config.Bot {
	for _, bot := range getConf().Bots {
		if bot.Mention != nil {
			continue
		}
		return bot
	}
	panic("expect at least one")
}

func getAppConf() *config.App {
	return getConf().App
}

func TestBot_Send(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	bot, err := New(getBotConf(), getAppConf(), nil)
	require.Nil(t, err)
	bot.openIDs = []string{"ou_177f84317c6ee52630edf335d5f8a6fc", "ou_177f84317c6ee52630edf335d5f8a6fc"}
	bot.titlePrefix = "[SHANGHAI]"
	bot.metadata = map[string]string{
		"链接": "https://www.baidu.com",
	}
	alerts := model.WebhookMessage{
		Data: newAlerts(),
		Meta: map[string]string{"group": "hello", "url": "www.baidu.com"},
	}
	err = bot.Send(&alerts)
	spew.Dump(err)
	require.Nil(t, err)
}

// fakeTransport 拦截 Sdk 的所有 HTTP 请求，依次返回预设响应（超出后重复最后一个）
type fakeTransport struct {
	mu        sync.Mutex
	responses []string
	methods   []string
	paths     []string
	bodies    []string
}

func (f *fakeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var body string
	if req.Body != nil {
		bs, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		body = string(bs)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.methods = append(f.methods, req.Method)
	f.paths = append(f.paths, req.URL.Path)
	f.bodies = append(f.bodies, body)
	idx := len(f.methods) - 1
	if idx >= len(f.responses) {
		idx = len(f.responses) - 1
	}
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader(f.responses[idx])),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
	}, nil
}

func newFakeBot(ft *fakeTransport) *Bot {
	sdk := NewSDK("", "")
	sdk.client = http.Client{Transport: ft}
	tpl, alertTpl, err := getTemplates(nil)
	if err != nil {
		panic(err)
	}
	return &Bot{
		chatID:   "oc_test",
		sdk:      sdk,
		tpl:      tpl,
		alertTpl: alertTpl,
		groups:   make(map[string]*groupState),
	}
}

func cardAlert(fp string, status string) template.Alert {
	return template.Alert{
		Status:      status,
		Fingerprint: fp,
		Annotations: map[string]string{"description": "alert " + fp},
		Labels:      map[string]string{"severity": "warn"},
		StartsAt:    time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func newCardMessage(groupKey string, alerts ...template.Alert) model.WebhookMessage {
	return model.WebhookMessage{
		Data: template.Data{
			Receiver:     "test",
			Alerts:       alerts,
			GroupLabels:  map[string]string{"alertname": "TestAlert"},
			ExternalURL:  "file://externalUrl",
			CommonLabels: map[string]string{"severity": "warn"},
		},
		GroupKey: groupKey,
	}
}

func TestSendCard_UpdateOnResolve(t *testing.T) {
	ft := &fakeTransport{responses: []string{
		`{"code":0,"msg":"success","data":{"message_id":"om_1"}}`,
	}}
	bot := newFakeBot(ft)

	// 首条 firing：发送新卡片
	msg := newCardMessage("gk1", cardAlert("fp1", "firing"))
	require.Nil(t, bot.Send(&msg))
	require.Equal(t, 1, len(ft.methods))
	require.Equal(t, "POST", ft.methods[0])
	require.Equal(t, "/open-apis/im/v1/messages", ft.paths[0])
	require.Contains(t, ft.bodies[0], "update_multi")
	require.Contains(t, ft.bodies[0], `red`)
	require.NotContains(t, ft.bodies[0], `green`)
	require.Equal(t, "om_1", bot.groups["gk1"].messageID)

	// resolved：编辑原卡片为绿色，而不是新发
	msg = newCardMessage("gk1", cardAlert("fp1", "resolved"))
	require.Nil(t, bot.Send(&msg))
	require.Equal(t, 2, len(ft.methods))
	require.Equal(t, "PATCH", ft.methods[1])
	require.Equal(t, "/open-apis/im/v1/messages/om_1", ft.paths[1])
	require.Contains(t, ft.bodies[1], `green`)
	// 全部恢复后状态清理
	require.Equal(t, 0, len(bot.groups))

	// 新一轮告警：重新发送新卡片
	msg = newCardMessage("gk1", cardAlert("fp2", "firing"))
	require.Nil(t, bot.Send(&msg))
	require.Equal(t, 3, len(ft.methods))
	require.Equal(t, "POST", ft.methods[2])
}

func TestSendCard_MergeAndPatchFail(t *testing.T) {
	ft := &fakeTransport{responses: []string{
		`{"code":0,"msg":"success","data":{"message_id":"om_1"}}`,
		`{"code":230110,"msg":"message not found"}`, // patch 失败（卡片被删）
		`{"code":0,"msg":"success","data":{"message_id":"om_2"}}`,
	}}
	bot := newFakeBot(ft)

	// 两条 firing 告警
	msg := newCardMessage("gk1", cardAlert("fp1", "firing"), cardAlert("fp2", "firing"))
	require.Nil(t, bot.Send(&msg))
	require.Equal(t, 1, len(ft.methods))

	// fp1 恢复：合并状态后编辑，卡片同时含 firing/resolved；patch 失败降级为新发
	msg = newCardMessage("gk1", cardAlert("fp1", "resolved"))
	require.Nil(t, bot.Send(&msg))
	require.Equal(t, 3, len(ft.methods))
	require.Equal(t, "PATCH", ft.methods[1])
	require.Contains(t, ft.bodies[1], "Firing")
	require.Contains(t, ft.bodies[1], "Resolved")
	require.Equal(t, "POST", ft.methods[2])
	require.Equal(t, "om_2", bot.groups["gk1"].messageID)

	// fp2 也恢复：编辑 om_2 后清理
	msg = newCardMessage("gk1", cardAlert("fp2", "resolved"))
	require.Nil(t, bot.Send(&msg))
	require.Equal(t, 4, len(ft.methods))
	require.Equal(t, "PATCH", ft.methods[3])
	require.Equal(t, "/open-apis/im/v1/messages/om_2", ft.paths[3])
	require.Equal(t, 0, len(bot.groups))
}

func TestCardContent(t *testing.T) {
	// 无 config 时自动注入 update_multi
	res, err := cardContent(`{"msg_type":"interactive","card":{"header":{"template":"red"}}}`)
	require.Nil(t, err)
	require.Contains(t, res, `"update_multi":true`)
	require.Contains(t, res, `"template":"red"`)

	// 已有 config 时保留其余字段
	res, err = cardContent(`{"msg_type":"interactive","card":{"config":{"wide_screen_mode":true},"header":{"template":"red"}}}`)
	require.Nil(t, err)
	require.Contains(t, res, `"update_multi":true`)
	require.Contains(t, res, `"wide_screen_mode":true`)

	// 非 JSON 模板输出报错
	_, err = cardContent(`not json`)
	require.NotNil(t, err)
}

// copyright: https://github.com/tomtom-international/alertmanager-webhook-logger/blob/master/main_test.go#L132
func newAlerts() template.Data {
	type Cat struct {
		Name  string
		BugMe string
	}
	bs, _ := json.Marshal(&Cat{
		Name: "cool cat",
	})
	bs, _ = json.Marshal(&Cat{
		Name:  "not cool cat",
		BugMe: string(bs),
	})
	return template.Data{
		Alerts: template.Alerts{
			template.Alert{
				Status: "firing",
				Annotations: map[string]string{
					"description": "26.09% throttling of CPU in namespace monitoring for container node-exporter in pod node-exporter-h5sjn" + string(bs),
					"runbook_url": "https://github.com/kubernetes-monitoring/kubernetes-mixin/tree/master/runbook.md#alert-name-cputhrottlinghigh",
					"summary":     "summary",
				},
				Labels:       map[string]string{"severity": "info", "m_key": "m_value"},
				StartsAt:     time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
				EndsAt:       time.Date(2000, 1, 1, 0, 0, 1, 0, time.UTC),
				GeneratorURL: "file://generatorUrl",
			},
			template.Alert{
				Annotations: map[string]string{
					"description": "\u001b26.09% throttling of CPU in namespace monitoring for container node-exporter in pod node-exporter-h5sjn",
				},
				Labels:   map[string]string{"l_key_warn": "l_value_warn"},
				Status:   "resolved",
				StartsAt: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		},
		CommonAnnotations: map[string]string{"ca_key": "ca_value"},
		CommonLabels:      map[string]string{"cl_key": "cl_value"},
		GroupLabels:       map[string]string{"gl_key": "gl_value"},
		ExternalURL:       "file://externalUrl",
		Receiver:          "test-receiver",
	}
}

func Test_mergeMap(t *testing.T) {
	type args struct {
		left  map[string]string
		right map[string]string
	}
	tests := []struct {
		name string
		args args
		want map[string]string
	}{
		{
			args: args{
				left: map[string]string{
					"a": "1",
					"b": "1",
				},
				right: map[string]string{
					"a": "2",
					"c": "2",
				},
			},
			want: map[string]string{
				"a": "1",
				"b": "1",
				"c": "2",
			},
		},
		{
			args: args{
				left:  nil,
				right: nil,
			},
			want: nil,
		},
		{
			args: args{
				left: nil,
				right: map[string]string{
					"a": "2",
					"c": "2",
				},
			},
			want: map[string]string{
				"a": "2",
				"c": "2",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mergeMap(tt.args.left, tt.args.right); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mergeMap() = %v, want %v", got, tt.want)
			}
		})
	}
}
