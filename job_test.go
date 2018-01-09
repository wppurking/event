package work

import (
	"testing"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

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
	j := Job{
		Name: "foobar",
		Delivery: &amqp.Delivery{
			Headers: amqp.Table{
				"x-death": []map[string]interface{}{
					{
						"count":               2,
						"exchange":            "work.work.schedule",
						"original-expiration": 35000,
						"queue":               "work._retry",
						"reason":              "expired",
						"routing-keys":        []string{"foobar"},
						"time":                1515465328,
					},
				},
			},
		},
	}
	assert.Equal(t, int64(2), j.Fails())

	j = Job{
		Name: "foobar",
		Delivery: &amqp.Delivery{
			Headers: amqp.Table{
				"x-death": []map[string]interface{}{
					{
						"count":               2,
						"exchange":            "work.work.schedule",
						"original-expiration": 35000,
						"queue":               "work._retry",
						"reason":              "expired",
						"routing-keys":        []string{"foobar"},
						"time":                1515465328,
					},
					{
						"count":               1,
						"exchange":            "work.work.schedule",
						"original-expiration": 35000,
						"queue":               "work._retry",
						"reason":              "expired",
						"routing-keys":        []string{"foobar"},
						"time":                1515465328,
					},
					{
						"count":               2,
						"exchange":            "work.work.schedule",
						"original-expiration": 35000,
						"queue":               "work._retry",
						"reason":              "expired",
						"routing-keys":        []string{"abc"},
						"time":                1515465328,
					},
				},
			},
		},
	}
	assert.Equal(t, int64(3), j.Fails())
	assert.Equal(t, int64(3), j.Fails())
}
