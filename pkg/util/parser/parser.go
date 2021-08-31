package parser

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"

	"gopkg.in/yaml.v2"

	"github.com/opentrx/seata-golang/v2/pkg/util/log"
)

// os环境变量
type envVar struct {
	name  string
	value string
}

type envVars []envVar

func (a envVars) Len() int           { return len(a) }
func (a envVars) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a envVars) Less(i, j int) bool { return a[i].name < a[j].name }

// Parser can be used to parse a configuration file and environment
// into a unified output structure
// 解析器可用于将配置文件和环境解析为统一的输出结构
type Parser struct {
	prefix string // 环境变量配置的前缀
	env    envVars
}

// NewParser returns a *Parser with the given environment prefix which handles
// configurations which match the given parseInfos
// NewParser返回一个带有给定环境前缀的*Parser，该前缀处理匹配给定parseInfos的配置
func NewParser(prefix string) *Parser {
	p := Parser{prefix: prefix}

	// Environ返回表示环境变量的格式为"key=value"的字符串的切片拷贝。
	for _, env := range os.Environ() {
		envParts := strings.SplitN(env, "=", 2)
		p.env = append(p.env, envVar{envParts[0], envParts[1]})
	}

	// We must sort the environment variables lexically by name so that
	// more specific variables are applied before less specific ones
	// but it's a lot simpler and easier to get right than unmarshalling
	// map entries into temporaries and merging with the existing entry.
	// 我们必须按名称对环境变量进行词法排序，以便在不太特定的变量之前应用更特定的变量，
	// 但这比将映射项解组到临时表项并与现有表项合并要简单得多。
	sort.Sort(p.env)

	return &p
}

// Parse reads in the given []byte and environment and writes the resulting
// configuration into the input v
//
// Environment variables may be used to override configuration parameters,
// following the scheme below:
// v.Abc may be replaced by the value of PREFIX_ABC,
// v.Abc.Xyz may be replaced by the value of PREFIX_ABC_XYZ, and so forth
// 使用环境变量替换配置文件
func (p *Parser) Parse(in []byte, v interface{}) error {
	err := yaml.Unmarshal(in, v)
	if err != nil {
		return err
	}

	// 环境变量替换
	for _, envVar := range p.env {
		pathStr := envVar.name
		if strings.HasPrefix(pathStr, strings.ToUpper(p.prefix)+"_") {
			path := strings.Split(pathStr, "_")

			err = p.overwriteFields(reflect.ValueOf(v), pathStr, path[1:], envVar.value)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// overwriteFields replaces configuration values with alternate values specified
// through the environment. Precondition: an empty path slice must never be
// passed in.
// 将配置值替换为通过环境指定的替代值。前提条件:不能传入空的路径。
func (p *Parser) overwriteFields(v reflect.Value, fullpath string, path []string, payload string) error {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			panic("encountered nil pointer while handling environment variable " + fullpath)
		}
		// 返回持有v持有的指针指向的值的Value
		v = reflect.Indirect(v)
	}
	switch v.Kind() {
	case reflect.Struct:
		return p.overwriteStruct(v, fullpath, path, payload)
	case reflect.Map:
		return p.overwriteMap(v, fullpath, path, payload)
	case reflect.Interface:
		if v.NumMethod() == 0 {
			if !v.IsNil() {
				return p.overwriteFields(v.Elem(), fullpath, path, payload)
			}
			// Interface was empty; create an implicit map
			var template map[string]interface{}
			wrappedV := reflect.MakeMap(reflect.TypeOf(template))
			v.Set(wrappedV)
			return p.overwriteMap(wrappedV, fullpath, path, payload)
		}
	}
	return nil
}

func (p *Parser) overwriteStruct(v reflect.Value, fullpath string, path []string, payload string) error {
	// Generate case-insensitive map of struct fields
	// 生成struct字段的不区分大小写的映射
	byUpperCase := make(map[string]int)
	for i := 0; i < v.NumField(); i++ {
		sf := v.Type().Field(i)
		upper := strings.ToUpper(sf.Name)
		// 配置项重复 大小写不同
		if _, present := byUpperCase[upper]; present {
			panic(fmt.Sprintf("field name collision in configuration object: %s", sf.Name))
		}
		byUpperCase[upper] = i
	}

	// 未找到要替换的路径
	fieldIndex, present := byUpperCase[path[0]]
	if !present {
		log.Warnf("ignoring unrecognized environment variable %s", fullpath)
		return nil
	}
	field := v.Field(fieldIndex)
	sf := v.Type().Field(fieldIndex)

	if len(path) == 1 {
		// Env var specifies this field directly
		// Env var直接指定该字段
		fieldVal := reflect.New(sf.Type)
		err := yaml.Unmarshal([]byte(payload), fieldVal.Interface())
		if err != nil {
			return err
		}
		field.Set(reflect.Indirect(fieldVal))
		return nil
	}

	// If the field is nil, must create an object
	switch sf.Type.Kind() {
	case reflect.Map:
		if field.IsNil() {
			field.Set(reflect.MakeMap(sf.Type))
		}
	case reflect.Ptr:
		if field.IsNil() {
			field.Set(reflect.New(field.Type().Elem()))
		}
	}

	err := p.overwriteFields(field, fullpath, path[1:], payload)
	if err != nil {
		return err
	}

	return nil
}

func (p *Parser) overwriteMap(m reflect.Value, fullpath string, path []string, payload string) error {
	if m.Type().Key().Kind() != reflect.String {
		// non-string keys unsupported
		log.Warnf("ignoring environment variable %s involving map with non-string keys", fullpath)
		return nil
	}

	if len(path) > 1 {
		// If a matching key exists, get its value and continue the
		// overwriting process.
		for _, k := range m.MapKeys() {
			if strings.ToUpper(k.String()) == path[0] {
				mapValue := m.MapIndex(k)
				// If the existing value is nil, we want to
				// recreate it instead of using this value.
				if (mapValue.Kind() == reflect.Ptr ||
					mapValue.Kind() == reflect.Interface ||
					mapValue.Kind() == reflect.Map) &&
					mapValue.IsNil() {
					break
				}
				return p.overwriteFields(mapValue, fullpath, path[1:], payload)
			}
		}
	}

	// (Re)create this key
	var mapValue reflect.Value
	if m.Type().Elem().Kind() == reflect.Map {
		mapValue = reflect.MakeMap(m.Type().Elem())
	} else {
		mapValue = reflect.New(m.Type().Elem())
	}
	if len(path) > 1 {
		err := p.overwriteFields(mapValue, fullpath, path[1:], payload)
		if err != nil {
			return err
		}
	} else {
		err := yaml.Unmarshal([]byte(payload), mapValue.Interface())
		if err != nil {
			return err
		}
	}

	m.SetMapIndex(reflect.ValueOf(strings.ToLower(path[0])), reflect.Indirect(mapValue))

	return nil
}
