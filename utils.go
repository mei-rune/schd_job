package schd_job

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Exportable interface {
	Stats() map[string]interface{}
}

func boolWithDefault(args map[string]interface{}, key string, defaultValue bool) bool {
	v, ok := args[key]
	if !ok {
		return defaultValue
	}
	if value, ok := v.(bool); ok {
		return value
	}

	s := fmt.Sprint(v)
	switch s {
	case "1", "true":
		return true
	case "0", "false":
		return false
	default:
		return defaultValue
	}
}

func intWithDefault(args map[string]interface{}, key string, defaultValue int) int {
	v, ok := args[key]
	if !ok {
		return defaultValue
	}
	switch value := v.(type) {
	case int:
		return value
	case int64:
		return int(value)
	case int32:
		return int(value)
	case string:
		i, e := strconv.ParseInt(value, 10, 0)
		if nil != e {
			return defaultValue
		}
		return int(i)
	default:
		s := fmt.Sprint(value)
		i, e := strconv.ParseInt(s, 10, 0)
		if nil != e {
			return defaultValue
		}
		return int(i)
	}
}
func durationWithDefault(args map[string]interface{}, key string, defaultValue time.Duration) time.Duration {
	v, ok := args[key]
	if !ok {
		return defaultValue
	}
	switch value := v.(type) {
	case time.Duration:
		return value
	case string:
		i, e := time.ParseDuration(value)
		if nil != e {
			return defaultValue
		}
		return i
	default:
		s := fmt.Sprint(value)
		i, e := time.ParseDuration(s)
		if nil != e {
			return defaultValue
		}
		return i
	}
}

func timeWithDefault(args map[string]interface{}, key string, defaultValue time.Time) time.Time {
	v, ok := args[key]
	if !ok {
		return defaultValue
	}
	switch value := v.(type) {
	case time.Time:
		return value
	case string:
		for _, layout := range []string{time.ANSIC,
			time.UnixDate,
			time.RubyDate,
			time.RFC822,
			time.RFC822Z,
			time.RFC850,
			time.RFC1123,
			time.RFC1123Z,
			time.RFC3339,
			time.RFC3339Nano} {
			t, e := time.Parse(value, layout)
			if nil == e {
				return t
			}
		}
		return defaultValue
	default:
		return defaultValue
	}
}

func stringWithDefault(args map[string]interface{}, key string, defaultValue string) string {
	v, ok := args[key]
	if !ok {
		return defaultValue
	}
	if nil == v {
		return defaultValue
	}
	if s, ok := v.(string); ok && 0 != len(s) {
		return s
	}
	return fmt.Sprint(v)
}

func stringsWithDefault(args map[string]interface{}, key, sep string, defaultValue []string) []string {
	v, ok := args[key]
	if !ok {
		return defaultValue
	}
	if ii, ok := v.([]interface{}); ok {
		ss := make([]string, len(ii))
		for i, s := range ii {
			ss[i] = fmt.Sprint(s)
		}
		return ss
	}
	if ss, ok := v.([]string); ok {
		return ss
	}
	if s, ok := v.(string); ok && 0 != len(s) {
		if 0 == len(sep) {
			return []string{s}
		}
		return strings.Split(s, sep)
	}
	return defaultValue
}

func mapWithDefault(args map[string]interface{}, key string, defaultValue map[string]interface{}) map[string]interface{} {
	v, ok := args[key]
	if !ok {
		return defaultValue
	}
	if m, ok := v.(map[string]interface{}); ok && nil != m {
		return m
	}
	return defaultValue
}

func boolWithArguments(arguments []map[string]interface{}, key string, defaultValue bool) bool {
	for _, arg := range arguments {
		v, ok := arg[key]
		if !ok {
			continue
		}
		if value, ok := v.(bool); ok {
			return value
		}

		s := fmt.Sprint(v)
		switch s {
		case "1", "true":
			return true
		case "0", "false":
			return false
		}
	}
	return defaultValue
}

func intWithArguments(arguments []map[string]interface{}, key string, defaultValue int) int {
	for _, arg := range arguments {
		v, ok := arg[key]
		if !ok {
			continue
		}

		switch value := v.(type) {
		case int:
			return value
		case int64:
			return int(value)
		case int32:
			return int(value)
		case string:
			i, e := strconv.ParseInt(value, 10, 0)
			if nil == e {
				return int(i)
			}
		default:
			s := fmt.Sprint(value)
			i, e := strconv.ParseInt(s, 10, 0)
			if nil == e {
				return int(i)
			}
		}
	}
	return defaultValue
}

func durationWithArguments(arguments []map[string]interface{}, key string, defaultValue time.Duration) time.Duration {
	for _, arg := range arguments {
		v, ok := arg[key]
		if !ok {
			continue
		}

		switch value := v.(type) {
		case time.Duration:
			return value
		case string:
			i, e := time.ParseDuration(value)
			if nil == e {
				return i
			}
		default:
			s := fmt.Sprint(value)
			i, e := time.ParseDuration(s)
			if nil == e {
				return i
			}
		}
	}
	return defaultValue
}

func timeWithArguments(arguments []map[string]interface{}, key string, defaultValue time.Time) time.Time {
	for _, arg := range arguments {
		v, ok := arg[key]
		if !ok {
			continue
		}
		switch value := v.(type) {
		case time.Time:
			return value
		case string:
			for _, layout := range []string{time.ANSIC,
				time.UnixDate,
				time.RubyDate,
				time.RFC822,
				time.RFC822Z,
				time.RFC850,
				time.RFC1123,
				time.RFC1123Z,
				time.RFC3339,
				time.RFC3339Nano} {
				t, e := time.Parse(value, layout)
				if nil == e {
					return t
				}
			}
		}
	}
	return defaultValue
}

func stringWithArguments(arguments []map[string]interface{}, key string, defaultValue string) string {
	for _, arg := range arguments {
		v, ok := arg[key]
		if !ok {
			continue
		}

		if s, ok := v.(string); ok && 0 != len(s) {
			return s
		}
		return fmt.Sprint(v)
	}
	return defaultValue
}

func stringsWithArguments(arguments []map[string]interface{}, key, sep string, defaultValue []string, is_merge bool) []string {
	if is_merge {
		values := defaultValue
		for _, arg := range arguments {
			ss := stringsWithDefault(arg, key, sep, nil)
			if nil != ss {
				values = append(values, ss...)
			}
		}
		return values
	}

	for _, arg := range arguments {
		v, ok := arg[key]
		if !ok {
			continue
		}

		if ii, ok := v.([]interface{}); ok {
			ss := make([]string, len(ii))
			for i, s := range ii {
				ss[i] = fmt.Sprint(s)
			}
			return ss
		}

		if ss, ok := v.([]string); ok {
			return ss
		}

		if s, ok := v.(string); ok && 0 != len(s) {
			if 0 == len(sep) {
				return []string{s}
			}
			return strings.Split(s, sep)
		}
	}
	return defaultValue
}

func ToFilename(name string) string {
	name = strings.Replace(name, "/", "_", -1)
	name = strings.Replace(name, "\\", "_", -1)
	name = strings.Replace(name, "*", "_", -1)
	name = strings.Replace(name, ":", "_", -1)
	name = strings.Replace(name, "\"", "_", -1)
	name = strings.Replace(name, "|", "_", -1)
	name = strings.Replace(name, "?", "_", -1)
	name = strings.Replace(name, ">", "_", -1)
	name = strings.Replace(name, "<", "_", -1)
	return name
}