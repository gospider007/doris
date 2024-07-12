package doris

import (
	"context"
	"errors"
	"fmt"

	"github.com/gospider007/requests"
)

type ClientOption struct {
	Host     string
	Port     int
	User     string
	Password string
}
type Client struct {
	baseUrl string
}

func (obj *Client) Insert(ctx context.Context, db, table string, datas ...any) error {
	return obj.streamLoad(ctx, db, table, false, datas...)
}
func (obj *Client) Delete(ctx context.Context, db, table string, datas ...any) error {
	return obj.streamLoad(ctx, db, table, true, datas...)
}

func (obj *Client) streamLoad(ctx context.Context, db, table string, isDelete bool, datas ...any) error {
	if len(datas) == 0 {
		return nil
	}
	headers := map[string]any{
		"Expect":            "100-continue",
		"format":            "json",
		"strip_outer_array": true,
		"ignore_json_size":  true,
		"strict_mode":       true,
	}
	if isDelete {
		headers["merge_type"] = "DELETE"
	}
	resp, err := requests.Put(ctx, fmt.Sprintf("%s/%s/%s/_stream_load", obj.baseUrl, db, table), requests.RequestOption{
		Headers: headers,
		Json:    datas,
	})
	if err != nil {
		return err
	}
	jsonData, err := resp.Json()
	if err != nil {
		return err
	}
	if jsonData.Get("Status").String() != "Success" {
		return errors.New(jsonData.String())
	}
	return nil
}

func NewClient(ctx context.Context, opt ClientOption) *Client {
	var userPass string
	if opt.User != "" && opt.Password != "" {
		userPass = fmt.Sprintf("%s:%s@", opt.User, opt.Password)
	}
	baseUrl := fmt.Sprintf("http://%s%s:%d/api", userPass, opt.Host, opt.Port)
	return &Client{baseUrl: baseUrl}
}
