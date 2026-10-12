package feishu

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/icza/gox/stringsx"
	amtemplate "github.com/prometheus/alertmanager/template"
	"github.com/sirupsen/logrus"
	"github.com/xujiahua/alertmanager-webhook-feishu/config"
	"github.com/xujiahua/alertmanager-webhook-feishu/feishu/rotate"
	"github.com/xujiahua/alertmanager-webhook-feishu/model"
	"github.com/xujiahua/alertmanager-webhook-feishu/tmpl"
	"sort"
	"strings"
	"sync"
	"text/template"
	"time"
)

type Bot struct {
	webhook  string
	chatID   string
	openIDs  []string
	rotator  *rotate.MentionRotator
	sdk      *Sdk
	tpl      *template.Template
	alertTpl *template.Template

	titlePrefix string
	metadata    map[string]string

	// chat_id 模式下，追踪每个告警组的卡片，后续通知（新 firing、resolved）编辑更新已有卡片
	mu     sync.Mutex
	groups map[string]*groupState
}

// groupState 记录一个告警组（GroupKey）当前卡片的状态
type groupState struct {
	messageID string
	// data 最新一次通知的头信息（GroupLabels 等），渲染时替换 Alerts
	data   model.WebhookMessage
	alerts map[string]amtemplate.Alert
}

func New(bot *config.Bot, app *config.App, helper *EmailHelper) (*Bot, error) {
	// @xxx
	openIDs, err := getOpenIDs(bot.Mention, helper)
	if err != nil {
		return nil, err
	}

	var rotator *rotate.MentionRotator
	if bot.Mention != nil && bot.Mention.Rotation != "" && len(openIDs) > 1 {
		rotator, err = rotate.New(bot.Mention.Rotation, openIDs)
		if err != nil {
			return nil, err
		}
	}

	// template
	tpl, alertTpl, err := getTemplates(bot.Template)
	if err != nil {
		return nil, err
	}

	var sdk *Sdk
	if bot.ChatID != "" {
		if app == nil || app.ID == "" || app.Secret == "" {
			return nil, errors.New("app.id and app.secret are required when bot chat_id is configured")
		}
		sdk = NewSDK(app.ID, app.Secret, app.Domain)
	} else {
		sdk = NewSDK("", "", "")
	}

	return &Bot{
		webhook:     bot.Webhook,
		chatID:      bot.ChatID,
		rotator:     rotator,
		openIDs:     openIDs,
		sdk:         sdk,
		tpl:         tpl,
		alertTpl:    alertTpl,
		titlePrefix: bot.TitlePrefix,
		metadata:    bot.MetaData,
		groups:      make(map[string]*groupState),
	}, nil
}

func getOpenIDs(mention *config.Mention, helper *EmailHelper) ([]string, error) {
	if mention == nil {
		return nil, nil
	}
	if mention.All {
		return []string{"all"}, nil
	}

	openIDs := mention.OpenIDs
	emails := mention.Emails
	if len(emails) != 0 && helper == nil {
		return nil, errors.New("@somebody by email need email flag enabled")
	}
	if len(emails) != 0 {
		remaining, err := helper.Lookup(emails)
		if err != nil {
			return nil, err
		}
		openIDs = append(openIDs, remaining...)
	}
	return openIDs, nil
}

func getTemplates(tmplConf *config.Template) (*template.Template, *template.Template, error) {
	if tmplConf != nil && tmplConf.CustomPath != "" {
		t, err := tmpl.GetCustomTemplate(tmplConf.CustomPath)
		if err != nil {
			return nil, nil, err
		}
		return t, nil, nil
	}

	// by default, use two tmpls, one is for alert
	dt, err := tmpl.GetEmbedTemplate("default.tmpl")
	if err != nil {
		return nil, nil, err
	}

	dat, err := tmpl.GetEmbedTemplate("default_alert.tmpl")
	if err != nil {
		return nil, nil, err
	}

	return dt, dat, nil
}

func (b *Bot) Send(alerts *model.WebhookMessage) error {
	// attach @xxx
	if b.rotator != nil {
		alerts.OpenIDs = b.rotator.Rotate(time.Now())
	} else {
		alerts.OpenIDs = b.openIDs
	}
	// title prefix
	alerts.TitlePrefix = b.titlePrefix

	// merge metadata
	alerts.Meta = mergeMap(alerts.Meta, b.metadata)

	if b.chatID != "" {
		return b.sendCard(alerts)
	}

	// prepare data
	err := b.preprocessAlerts(alerts)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	err = b.tpl.Execute(&buf, alerts)
	if err != nil {
		return err
	}
	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		if d, err := beautifyJSON(buf.String()); err != nil {
			logrus.Error(err)
			fmt.Println(buf.String())
		} else {
			fmt.Println(d)
		}
	}

	return b.sdk.WebhookV2(b.webhook, &buf)
}

// sendCard 以应用身份发送/更新卡片。
// 同一告警组（GroupKey）只发一张卡片：首条通知发送新卡片并记录 message_id，
// 后续通知（新 firing、resolved）合并状态后 PATCH 编辑原卡片；
// 全部恢复后清理状态，下一轮告警发送新卡片。
func (b *Bot) sendCard(alerts *model.WebhookMessage) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	st, ok := b.groups[alerts.GroupKey]
	if !ok {
		st = &groupState{alerts: make(map[string]amtemplate.Alert)}
		b.groups[alerts.GroupKey] = st
	}
	// 以最新通知为准更新各 alert 状态
	for _, a := range alerts.Alerts {
		st.alerts[a.Fingerprint] = a
	}
	// 保存头信息（GroupLabels 等）
	base := *alerts
	base.Alerts = nil
	base.FiringAlerts = nil
	base.ResolvedAlerts = nil
	st.data = base

	msg := st.data
	msg.Alerts = sortedAlerts(st.alerts)

	content, err := b.renderCard(&msg)
	if err != nil {
		return err
	}

	if st.messageID == "" {
		messageID, err := b.sdk.SendMessage(b.chatID, content)
		if err != nil {
			return err
		}
		st.messageID = messageID
	} else {
		err := b.sdk.PatchCard(st.messageID, content)
		if err != nil {
			// 卡片可能已被删除或超过可编辑时限（14 天），降级为重新发送
			logrus.Warnf("patch card %s failed, resend instead, %s", st.messageID, err)
			messageID, serr := b.sdk.SendMessage(b.chatID, content)
			if serr != nil {
				return serr
			}
			st.messageID = messageID
		}
	}

	// 全部恢复后清理状态，下一轮告警发送新卡片
	if len(msg.Alerts) != 0 && len(msg.Alerts.Resolved()) == len(msg.Alerts) {
		delete(b.groups, alerts.GroupKey)
	}

	return nil
}

func (b *Bot) renderCard(alerts *model.WebhookMessage) (string, error) {
	err := b.preprocessAlerts(alerts)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	err = b.tpl.Execute(&buf, alerts)
	if err != nil {
		return "", err
	}
	if logrus.IsLevelEnabled(logrus.DebugLevel) {
		if d, err := beautifyJSON(buf.String()); err != nil {
			logrus.Error(err)
			fmt.Println(buf.String())
		} else {
			fmt.Println(d)
		}
	}
	return cardContent(buf.String())
}

// cardContent 从 webhook 格式的渲染结果中提取卡片 JSON，
// 并确保 config.update_multi=true（PATCH 更新卡片的前置条件）
func cardContent(webhookBody string) (string, error) {
	var body struct {
		Card json.RawMessage `json:"card"`
	}
	err := json.Unmarshal([]byte(webhookBody), &body)
	if err != nil {
		return "", err
	}
	if len(body.Card) == 0 {
		return "", errors.New("no card found in template output")
	}

	var card map[string]json.RawMessage
	err = json.Unmarshal(body.Card, &card)
	if err != nil {
		return "", err
	}

	if cfg, ok := card["config"]; ok {
		var m map[string]interface{}
		if err = json.Unmarshal(cfg, &m); err != nil {
			return "", err
		}
		m["update_multi"] = true
		nb, err := json.Marshal(m)
		if err != nil {
			return "", err
		}
		card["config"] = nb
	} else {
		card["config"] = json.RawMessage(`{"update_multi":true}`)
	}

	res, err := json.Marshal(card)
	if err != nil {
		return "", err
	}
	return string(res), nil
}

// sortedAlerts 稳定排序，避免编辑卡片时条目顺序跳动
func sortedAlerts(alerts map[string]amtemplate.Alert) amtemplate.Alerts {
	res := make(amtemplate.Alerts, 0, len(alerts))
	for _, a := range alerts {
		res = append(res, a)
	}
	sort.SliceStable(res, func(i, j int) bool {
		if res[i].StartsAt.Equal(res[j].StartsAt) {
			return res[i].Fingerprint < res[j].Fingerprint
		}
		return res[i].StartsAt.Before(res[j].StartsAt)
	})
	return res
}

// right is immutable
func mergeMap(left, right map[string]string) map[string]string {
	if len(right) == 0 {
		return left
	}
	if left == nil {
		left = make(map[string]string)
	}
	for k, v := range right {
		if _, ok := left[k]; !ok {
			left[k] = v
		}
	}
	return left
}

// field description may contain double quote, non printable chars
func fixDescription(s string) string {
	// feishu fix: clean non printable char
	s = stringsx.Clean(s)
	// feishu fix: unescape a string
	s = fmt.Sprintf("%#v", s)
	// remove prefix and suffix double quote, means we just unescape inner text
	s = strings.TrimPrefix(s, "\"")
	s = strings.TrimSuffix(s, "\"")
	return s
}

func (b *Bot) preprocessAlerts(alerts *model.WebhookMessage) error {
	if b.alertTpl == nil {
		return nil
	}

	// preprocess using alert template
	// 注意：app 模式下 alert 会被缓存复用，clone 防止 fixDescription 的转义写回缓存（二次渲染会双重转义）
	for _, alert := range alerts.Alerts.Firing() {
		alert = cloneAlert(alert)
		var buf bytes.Buffer
		if _, ok := alert.Annotations["description"]; ok {
			alert.Annotations["description"] = fixDescription(alert.Annotations["description"])
		}
		err := b.alertTpl.Execute(&buf, alert)
		if err != nil {
			return err
		}
		res := strings.ReplaceAll(buf.String(), "\n", "\\n")
		alerts.FiringAlerts = append(alerts.FiringAlerts, res)
	}
	for _, alert := range alerts.Alerts.Resolved() {
		alert = cloneAlert(alert)
		var buf bytes.Buffer
		if _, ok := alert.Annotations["description"]; ok {
			alert.Annotations["description"] = fixDescription(alert.Annotations["description"])
		}
		err := b.alertTpl.Execute(&buf, alert)
		if err != nil {
			return err
		}
		res := strings.ReplaceAll(buf.String(), "\n", "\\n")
		alerts.ResolvedAlerts = append(alerts.ResolvedAlerts, res)
	}

	return nil
}

// cloneAlert 值拷贝 alert 及其 Annotations（map 是引用类型）
func cloneAlert(a amtemplate.Alert) amtemplate.Alert {
	c := a
	if a.Annotations != nil {
		c.Annotations = make(amtemplate.KV, len(a.Annotations))
		for k, v := range a.Annotations {
			c.Annotations[k] = v
		}
	}
	return c
}

func beautifyJSON(raw string) (string, error) {
	data := make(map[string]interface{})
	err := json.Unmarshal([]byte(raw), &data)
	if err != nil {
		return "", err
	}
	d, err := json.MarshalIndent(data, "", "\t")
	if err != nil {
		return "", err
	}
	return string(d), nil
}
