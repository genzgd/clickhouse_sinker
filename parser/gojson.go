package parser

import (
	"github.com/goccy/go-json"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/stretchr/objx"
	"github.com/thanos-io/thanos/pkg/errors"
	"regexp"
	"sync"
)

type GoJsonParser struct {
	pool *Pool
}

type GoJsonMetric struct {
	pool  *Pool
	value objx.Map
}

func (g GoJsonMetric) GetBool(key string, nullable bool) (val interface{}) {
	return g.value.Get(key).Bool()
}

func (g GoJsonMetric) GetInt8(key string, nullable bool) (val interface{}) {
	return g.value.Get(key).Int8()
}

func (g GoJsonMetric) GetInt16(key string, nullable bool) (val interface{}) {
	return g.value.Get(key).Int16
}

func (g GoJsonMetric) GetInt32(key string, nullable bool) (val interface{}) {
	return g.value.Get(key).Int32
}

func (g GoJsonMetric) GetInt64(key string, nullable bool) (val interface{}) {
	return g.value.Get(key).Int64
}

func (g GoJsonMetric) GetUint8(key string, nullable bool) (val interface{}) {
	return g.value.Get(key).Uint8()
}

func (g GoJsonMetric) GetUint16(key string, nullable bool) (val interface{}) {
	return g.value.Get(key).Uint16()
}

func (g GoJsonMetric) GetUint32(key string, nullable bool) (val interface{}) {
	return g.value.Get(key).Uint32()
}

func (g GoJsonMetric) GetUint64(key string, nullable bool) (val interface{}) {
	return g.value.Get(key).Uint64()
}

func (g GoJsonMetric) GetFloat32(key string, nullable bool) (val interface{}) {
	return g.value.Get(key).Float32()
}

func (g GoJsonMetric) GetFloat64(key string, nullable bool) (val interface{}) {
	return g.value.Get(key).Float64()
}

func (g GoJsonMetric) GetDecimal(key string, nullable bool) (val interface{}) {
	return nil
}

func (g GoJsonMetric) GetDateTime(key string, nullable bool) (val interface{}) {
	v := g.value.Get(key).String()
	if v == "" {
		return nil
	}
	t, err := g.pool.ParseDateTime(key, v)
	if err != nil {
		return Epoch
	}
	return t
}

func (g GoJsonMetric) GetString(key string, nullable bool) (val interface{}) {
	return g.value.Get(key).String()
}

func (g GoJsonMetric) GetObject(key string, nullable bool) (val interface{}) {
	return nil
}

func (g GoJsonMetric) GetMap(key string, typeinfo *model.TypeInfo) (val interface{}) {
	return nil
}

func (g GoJsonMetric) GetArray(key string, t int) (val interface{}) {
	return nil
}

func (g GoJsonMetric) GetNewKeys(knownKeys, newKeys, warnKeys *sync.Map, white, black *regexp.Regexp, partition int, offset int64) bool {
	return false
}

func (p *GoJsonParser) Parse(bs []byte) (metric model.Metric, err error) {
	var value map[string]interface{}
	if e1 := json.Unmarshal(bs, &value); e1 != nil {
		err = errors.Wrapf(e1, "")
		return
	}
	metric = &GoJsonMetric{value: value, pool: p.pool}
	return
}
