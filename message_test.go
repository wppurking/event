package work

import (
	"fmt"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

type TestContext struct{}

// 从 amqp.Table 中获取的值, 是有值的白名单的
func TestXdeathHeader(t *testing.T) {
	/**
	rabbitmq:
	headers:
		x-death:
			count:	2
			exchange:	work.work.schedule
			original-expiration:	35000
			queue:	work._retry
			reason:	expired
			routing-keys:	foobar
			time:	1515465328

	golang:
	{map[x-death:[map[time:2018-01-09 10:35:28 +0800 CST count:2 exchange:work.work.schedule original-expiration:35000 queue:work._retry reason:expired routing-keys:[foobar]]]] application/json  2 0   35000 948867e8b2d3d9f8a6a67568 2018-01-09 10:38:20.167374085 +0800 CST m=+0.115647621    [123 34 105 34 58 49 56 125]}
	*/

	/*
		exchange type is: string
		original-expiration type is: string
		queue type is: string
		reason type is: string
		routing-keys type is: []interface {}
		time type is: time.Time
		count type is: int64
	*/

	tb := amqp.Table{
		"count":               int64(2),
		"exchange":            "work.work.schedule",
		"original-expiration": "35000",
		"queue":               "work._retry",
		"reason":              "expired",
		"routing-keys":        []interface{}{"foobar"},
		"time":                time.Unix(1515465328, 0),
	}

	tb2 := amqp.Table{
		"count":               int32(1),
		"exchange":            "work.work.schedule",
		"original-expiration": "35000",
		"queue":               "work._retry",
		"reason":              "expired",
		"routing-keys":        []interface{}{"foobar"},
		"time":                time.Unix(1515465328, 0),
	}
	tb3 := amqp.Table{
		"count":               int16(2),
		"exchange":            "work.work.schedule",
		"original-expiration": "35000",
		"queue":               "work._retry",
		"reason":              "expired",
		"routing-keys":        []interface{}{"abc"},
		"time":                time.Unix(1515465328, 0),
	}

	// 三个 amqp.Table 需要符合约束
	assert.NoError(t, tb.Validate())
	assert.NoError(t, tb2.Validate())
	assert.NoError(t, tb3.Validate())

	j := Message{
		Name: "foobar",
		Delivery: &amqp.Delivery{
			Headers: amqp.Table{
				"x-death": []interface{}{tb},
			},
		},
	}
	assert.Equal(t, int64(2), j.Fails())

	j = Message{
		Name: "foobar",
		Delivery: &amqp.Delivery{
			Headers: amqp.Table{
				"x-death": []interface{}{tb, tb2, tb3},
			},
		},
	}
	assert.Equal(t, int64(3), j.Fails())
	assert.Equal(t, int64(3), j.Fails())
	fmt.Println(j.Headers)
}
